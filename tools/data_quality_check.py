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
import time
from queue import Queue, Empty
from threading import Thread
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine, should_use_kline_engine


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
    query_engine = get_kline_engine() if should_use_kline_engine(sql) else engine
    with query_engine.connect() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _row(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query_engine = get_kline_engine() if should_use_kline_engine(sql) else engine
    with query_engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        row = result.mappings().first()
        return dict(row) if row else {}


def _rows(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    query_engine = get_kline_engine() if should_use_kline_engine(sql) else engine
    with query_engine.connect() as conn:
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
    # Match the production (k_type, adjust_type, trade_date) index so the
    # readiness endpoint does not scan the full historical K-line table.
    d = _scalar(
        engine,
        "SELECT trade_date FROM sm_stock_kline "
        "WHERE k_type = 1 AND adjust_type = 0 "
        "ORDER BY trade_date DESC LIMIT 1",
    )
    return _fmt_date(d)


def expected_latest_trade_date(engine: Engine, as_of: date | None = None) -> str:
    """Latest trading day that should already have a completed daily K-line."""
    if as_of is None:
        now = datetime.now()
        as_of = now.date()
        is_today_trade_day = bool(_scalar(engine, """
            SELECT COUNT(*)
            FROM si_trade_calendar
            WHERE trade_date = :d
              AND trade_status = 1
        """, {"d": as_of.isoformat()}) or 0)
        if is_today_trade_day and now.hour < 18:
            d = _scalar(engine, """
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date < :as_of
            """, {"as_of": as_of.isoformat()})
            return _fmt_date(d)
    d = _scalar(engine, """
        SELECT MAX(trade_date)
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date <= :as_of
    """, {"as_of": as_of.isoformat()})
    return _fmt_date(d)


def expected_completed_trade_date(
    engine: Engine,
    now: datetime | None = None,
    ready_time: str = "15:20",
) -> str:
    """Latest trade date whose end-of-day data should already be available."""
    now = now or datetime.now()
    try:
        ready_hour, ready_minute = (int(part) for part in ready_time.split(":", 1))
    except (TypeError, ValueError):
        ready_hour, ready_minute = 15, 20
    comparator = "<=" if (now.hour, now.minute) >= (ready_hour, ready_minute) else "<"
    d = _scalar(engine, f"""
        SELECT MAX(trade_date)
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date {comparator} :today
    """, {"today": now.date().isoformat()})
    return _fmt_date(d)


def expected_intraday_date(engine: Engine, fallback_trade_date: str) -> str:
    """Today once intraday collection can exist; otherwise fallback date."""
    now = datetime.now()
    today = now.date().isoformat()
    is_trade_day = bool(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_trade_calendar
        WHERE trade_date = :d
          AND trade_status = 1
    """, {"d": today}) or 0)
    current = now.hour * 100 + now.minute
    return today if is_trade_day and current >= 925 else fallback_trade_date


def expected_scheduled_trade_date(
    engine: Engine,
    fallback_trade_date: str,
    *,
    ready_time: str,
    now: datetime | None = None,
) -> str:
    """Latest trade date expected after a dataset-specific publish window.

    Index and concept snapshots are post-close jobs, not intraday feeds.  A
    generic "today is a trade day" check makes them look stale all morning.
    Before the job's ready time, require the prior completed trading day;
    afterwards require today (when it is an open trading day).
    """
    now = now or datetime.now()
    try:
        ready_hour, ready_minute = (int(part) for part in ready_time.split(":", 1))
    except (TypeError, ValueError):
        ready_hour, ready_minute = 18, 0
    comparator = "<=" if (now.hour, now.minute) >= (ready_hour, ready_minute) else "<"
    expected = _scalar(engine, f"""
        SELECT MAX(trade_date)
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date {comparator} :today
    """, {"today": now.date().isoformat()})
    return _fmt_date(expected) or fallback_trade_date


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
        check_intraday_scheduler_health(engine),
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


def check_latest_trade_date_freshness(
    engine: Engine,
    trade_date: str,
    now: datetime | None = None,
) -> CheckResult:
    expected = expected_completed_trade_date(engine, now=now)
    if not expected:
        return CheckResult(
            name="latest_trade_date_freshness",
            status="WARN",
            message="交易日历缺少可用交易日，无法判断日K最新性",
            details={"actual_trade_date": trade_date, "expected_trade_date": ""},
        )

    lag_days = _date_lag_days(trade_date, expected)
    # A completed daily load may legitimately land before the conservative
    # cut-off used by ``expected_latest_trade_date`` (18:00 by default).  A
    # negative lag therefore means the data is ahead, not stale.
    ok = bool(trade_date) and lag_days <= 0
    ahead_days = max(0, -lag_days) if lag_days != 9999 else 0
    if ok and ahead_days:
        message = f"日K已领先保守预期：当前 {trade_date}，最低应到 {expected}"
    elif ok:
        message = "日K已跟上最新交易日"
    else:
        message = f"日K滞后：当前 {trade_date or '-'}，应到 {expected}"
    return CheckResult(
        name="latest_trade_date_freshness",
        status=_status(ok),
        message=message,
        details={
            "actual_trade_date": trade_date,
            "expected_trade_date": expected,
            "lag_calendar_days": lag_days,
            "ahead_calendar_days": ahead_days,
        },
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
    ok = active_count > 0 and kline_count >= 5000 and ratio >= 0.90
    warn = ok and ratio < 0.92
    return CheckResult(
        name="daily_kline_coverage",
        status=_status(ok, warn),
        message=f"{trade_date} 日K覆盖 {kline_count}/{active_count} ({ratio:.1%})",
        details={
            "trade_date": trade_date,
            "active_count": active_count,
            "kline_count": kline_count,
            "coverage": ratio,
            "coverage_basis": "si_all_code may include historical and delisted codes",
        },
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
    hard_issue_count = sum(int(row.get(k) or 0) for k in ("null_ohlc", "bad_ohlc", "nonpositive_price"))
    extreme_count = int(row.get("extreme_change") or 0)
    issue_count = hard_issue_count + extreme_count
    ok = total > 0 and hard_issue_count == 0
    return CheckResult(
        name="daily_kline_integrity",
        status=_status(ok, warn=ok and extreme_count > 0),
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


def check_recent_kline_calendar_completeness(
    engine: Engine,
    trade_date: str,
    *,
    lookback: int | None = None,
) -> CheckResult:
    """Fail when an open session is absent from the recent daily K-line range."""
    lookback = lookback or max(5, int(os.environ.get("DQ_KLINE_CALENDAR_LOOKBACK", "30")))
    calendar_rows = _rows(engine, """
        SELECT trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date <= :d
        ORDER BY trade_date DESC
        LIMIT :lookback
    """, {"d": trade_date, "lookback": lookback})
    expected = sorted(_fmt_date(row.get("trade_date")) for row in calendar_rows if row.get("trade_date"))
    if not expected:
        return CheckResult(
            name="recent_kline_calendar_completeness",
            status="FAIL",
            message="交易日历没有可用于日 K 连续性检查的开市日",
            details={"trade_date": trade_date, "lookback": lookback, "missing_dates": []},
        )

    count_rows = _rows(engine, """
        SELECT trade_date, COUNT(DISTINCT stock_code) AS stock_count
        FROM sm_stock_kline
        WHERE trade_date BETWEEN :start_date AND :end_date
          AND k_type = 1
          AND adjust_type = 0
        GROUP BY trade_date
        ORDER BY trade_date
    """, {"start_date": expected[0], "end_date": expected[-1]})
    counts = {
        _fmt_date(row.get("trade_date")): int(row.get("stock_count") or 0)
        for row in count_rows
    }
    missing = [value for value in expected if counts.get(value, 0) == 0]
    thin = [
        {"trade_date": value, "stock_count": counts.get(value, 0)}
        for value in expected
        if 0 < counts.get(value, 0) < 1000
    ]
    ok = not missing and not thin
    message = (
        f"最近 {len(expected)} 个开市日日 K 连续"
        if ok
        else f"最近开市日日 K 缺口 {len(missing)} 天、低覆盖 {len(thin)} 天"
    )
    return CheckResult(
        name="recent_kline_calendar_completeness",
        status=_status(ok),
        message=message,
        details={
            "trade_date": trade_date,
            "lookback": len(expected),
            "range_start": expected[0],
            "range_end": expected[-1],
            "missing_dates": missing,
            "thin_dates": thin,
            "daily_stock_counts": counts,
        },
    )


def check_flow_coverage(engine: Engine, trade_date: str) -> CheckResult:
    # Compare keys on the requested date. A newer partition or a large row
    # count must not hide missing stocks, and unsupported Beijing flow must
    # not lower the provider-supported denominator.
    expected_rows = _rows(engine, """
        SELECT stock_code
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
          AND volume > 0 AND stock_code REGEXP '^(00|30|60|68)'
    """, {"d": trade_date})
    flow_rows = _rows(engine, """
        SELECT stock_code, data_source,
               CASE WHEN main_net_inflow IS NOT NULL
                     AND max_net_inflow IS NOT NULL AND lg_net_inflow IS NOT NULL
                     AND mid_net_inflow IS NOT NULL AND sm_net_inflow IS NOT NULL
                    THEN 1 ELSE 0 END AS fields_present
        FROM sm_stock_capital_flow_daily WHERE trade_date = :d
    """, {"d": trade_date})
    expected = {str(row["stock_code"]).zfill(6) for row in expected_rows}
    actual = {str(row["stock_code"]).zfill(6) for row in flow_rows}
    missing = sorted(expected - actual)
    invalid = sorted({
        str(row["stock_code"]).zfill(6) for row in flow_rows
        if str(row["stock_code"]).zfill(6) in expected
        and not row.get("fields_present")
    })
    sources: dict[str, int] = {}
    for row in flow_rows:
        if str(row["stock_code"]).zfill(6) in expected:
            source = str(row.get("data_source") or "UNKNOWN")
            sources[source] = sources.get(source, 0) + 1
    matched = len(expected & actual)
    ratio = round(matched / len(expected), 4) if expected else 0
    ok = bool(expected) and not missing and not invalid
    return CheckResult(
        name="capital_flow_coverage",
        status=_status(ok, warn=ok and (len(sources) > 1 or "UNKNOWN" in sources)),
        message=f"{trade_date} 资金流支持范围覆盖 {matched}/{len(expected)}，缺失 {len(missing)}，空字段 {len(invalid)}",
        details={"trade_date": trade_date, "flow_date": trade_date if actual else "",
                 "flow_count": matched, "kline_count": len(expected), "coverage": ratio,
                 "coverage_basis": "target_date_traded_unadjusted_daily_supported_markets",
                 "missing_codes": missing, "invalid_codes": invalid,
                 "source_counts": sources, "mixed_sources": len(sources) > 1,
                 "outside_expected_count": len(actual - expected),
                 "prerequisite_missing": not expected},
    )


def check_acquisition_calendar(engine: Engine, *, now: datetime) -> CheckResult:
    horizon = now.date() + timedelta(days=7)
    rows = _rows(engine, """
        SELECT trade_date, trade_status FROM si_trade_calendar
        WHERE trade_date BETWEEN :start AND :end ORDER BY trade_date
    """, {"start": now.date().isoformat(), "end": horizon.isoformat()})
    actual = {_fmt_date(row["trade_date"]): row.get("trade_status") for row in rows}
    expected = [(now.date() + timedelta(days=i)).isoformat() for i in range(8)]
    missing = [day for day in expected if actual.get(day) not in (0, 1)]
    return CheckResult(
        "acquisition_calendar", _status(not missing),
        "未来一周交易日历已覆盖" if not missing else "交易日历缺失或状态无效",
        {"through": horizon.isoformat(), "missing_dates": missing},
    )


def check_acquisition_executors(engine: Engine) -> CheckResult:
    # Observability only: this never grants writer authority or substitutes
    # for the release-bound PID/SHA/fencing checks in scheduler_runtime_health.
    rows = _rows(engine, """
        SELECT executor_role, instance_id, heartbeat_at, build_sha,
               TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS age_seconds
        FROM st_scheduler_runtime
        WHERE executor_role IN ('linux_standalone', 'qmt_windows_edge')
        ORDER BY heartbeat_at DESC
    """)
    details = {}
    for role in ("linux_standalone", "qmt_windows_edge"):
        selected = [row for row in rows if row["executor_role"] == role]
        fresh = [row for row in selected if row.get("age_seconds") is not None
                 and 0 <= int(row["age_seconds"]) <= 120]
        future = any(row.get("age_seconds") is not None
                     and int(row["age_seconds"]) < 0 for row in selected)
        details[role] = {"ready": len(fresh) == 1 and not future,
                         "fresh_count": len(fresh), "future_heartbeat": future,
                         "latest": selected[0] if selected else None}
    ready = all(item["ready"] for item in details.values())
    return CheckResult("acquisition_executors", _status(ready),
                       "两端采集调度心跳正常" if ready else "采集调度器缺失、心跳过期或存在冲突",
                       {"observation_only": True, "executors": details})


def check_recent_flow_calendar_completeness(engine: Engine, trade_date: str) -> CheckResult:
    calendar = _rows(engine, """
        SELECT trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date<=:d
        ORDER BY trade_date DESC LIMIT 21
    """, {"d": trade_date})
    expected = sorted({_fmt_date(row["trade_date"]) for row in calendar})
    counts = {}
    if expected:
        rows = _rows(engine, """
            SELECT trade_date, COUNT(DISTINCT stock_code) AS stock_count
            FROM sm_stock_capital_flow_daily
            WHERE trade_date BETWEEN :start AND :end
              AND stock_code REGEXP '^(00|30|60|68)'
            GROUP BY trade_date
        """, {"start": expected[0], "end": expected[-1]})
        counts = {_fmt_date(row["trade_date"]): int(row["stock_count"]) for row in rows}
    missing = [day for day in expected if counts.get(day, 0) == 0]
    return CheckResult(
        "recent_flow_calendar_completeness", _status(bool(expected) and not missing),
        f"近 {len(expected)} 个交易日资金流整日缺口 {len(missing)}；非逐股历史完整性认证",
        {"missing_dates": missing, "daily_stock_counts": counts,
         "coverage_basis": "calendar_partition_presence_only", "lookback": len(expected)},
    )


def _bounded_acquisition_check(operation, timeout_seconds=8.0):
    """Only read-only checks: a stuck query cannot hide all later results."""
    outcome = Queue(maxsize=1)
    def run():
        try:
            outcome.put((True, operation()))
        except Exception as exc:
            outcome.put((False, exc))
    Thread(target=run, daemon=True, name="probiga-acquisition-check").start()
    try:
        ok, value = outcome.get(timeout=timeout_seconds)
    except Empty:
        raise TimeoutError("read-only acquisition check exceeded its budget") from None
    if not ok:
        raise value
    return value


def run_acquisition_checks(engine: Engine, trade_date: str | None = None) -> dict[str, Any]:
    """Bounded read-only acquisition report, independent of strategy results.

    Aggregate failures instead of stopping at the first failing provider.
    This is not a canonical publication or trading-readiness authorization.
    """
    now = datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    checks = []
    try:
        target = trade_date or _bounded_acquisition_check(
            lambda: expected_completed_trade_date(engine, now=now, ready_time="18:00"))
        if not target or date.fromisoformat(target).isoformat() != target:
            raise ValueError("invalid acquisition target")
    except Exception as exc:
        target = ""
        checks.append(CheckResult("acquisition_target", "FAIL", "应完成交易日无法确定",
                                  {"error_type": type(exc).__name__}))
    operations = [
        ("acquisition_calendar", lambda: check_acquisition_calendar(engine, now=now)),
        ("acquisition_executors", lambda: check_acquisition_executors(engine)),
        ("latest_trade_date_freshness", lambda: CheckResult(
            "latest_trade_date_freshness", _status(bool(target) and latest_trade_date(engine) >= target),
            "日线最新日期须达到应完成交易日", {"expected_trade_date": target})),
    ]
    if target:
        operations.extend([
            ("recent_kline_calendar_completeness", lambda: check_recent_kline_calendar_completeness(engine, target, lookback=21)),
            ("recent_flow_calendar_completeness", lambda: check_recent_flow_calendar_completeness(engine, target)),
            ("capital_flow_coverage", lambda: check_flow_coverage(engine, target)),
        ])
    for name, operation in operations:
        started = time.monotonic()
        print(f"acquisition-check start: {name}", file=sys.stderr, flush=True)
        try:
            result = _bounded_acquisition_check(operation)
        except Exception as exc:
            # DB/connector exceptions may contain credentials or signed URLs.
            result = CheckResult(name, "FAIL", "只读检查失败或超时，不能视为数据可用",
                                 {"error_type": type(exc).__name__})
        elapsed = round(time.monotonic() - started, 3)
        checks.append(CheckResult(result.name, result.status, result.message,
                                  {**(result.details or {}), "elapsed_seconds": elapsed}))
        print(f"acquisition-check end: {name} {result.status} {elapsed}s", file=sys.stderr, flush=True)
    statuses = {item.status for item in checks}
    return {"status": "FAIL" if "FAIL" in statuses else "WARN" if "WARN" in statuses else "PASS",
            "trade_date": target, "generated_at": now.isoformat(timespec="seconds"),
            "scope": "acquisition_observation_not_publication_authority",
            "not_checked": ["historical_per_stock_completeness", "minute_session_completeness",
                            "announcement_finance_membership_evidence", "publication_authority"],
            "checks": [item.as_dict() for item in checks]}


def check_realtime_freshness(engine: Engine, trade_date: str) -> CheckResult:
    if not _table_exists(engine, "sm_rt_quote_snapshot"):
        return CheckResult("realtime_snapshot", "WARN", "缺少 sm_rt_quote_snapshot，盘中模拟会降级到其它行情源")
    intraday_date = expected_intraday_date(engine, trade_date)
    day_start = datetime.strptime(intraday_date, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    row = _row(engine, """
        SELECT MAX(snapshot_at) AS latest_snapshot, COUNT(DISTINCT stock_code) AS stock_count
        FROM sm_rt_quote_snapshot
        WHERE snapshot_at >= :day_start
          AND snapshot_at < :day_end
    """, {"day_start": day_start, "day_end": day_end})
    latest = row.get("latest_snapshot")
    stock_count = int(row.get("stock_count") or 0)
    ok = latest is not None and stock_count >= 1000
    warn = ok and stock_count < 4000
    msg = f"实时快照 {intraday_date} 覆盖 {stock_count} 只，最新 {latest or '-'}"
    return CheckResult("realtime_snapshot", _status(ok, warn), msg, row)


def _expected_intraday_stock_count(engine: Engine) -> tuple[int, str, str]:
    """Return a stable full-market denominator for intraday coverage.

    Intraday checks used to count whatever happened to exist on the requested
    K-line date.  A partial daily load therefore lowered the denominator and
    could make an equally partial realtime load look healthy.  Use the latest
    completed, unadjusted daily K-line universe instead, with the reference
    universe as an explicit fallback.
    """
    row = _row(engine, """
        SELECT trade_date, COUNT(DISTINCT stock_code) AS stock_count
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND trade_date = (
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1 AND adjust_type = 0
          )
        GROUP BY trade_date
    """)
    count = int(row.get("stock_count") or 0)
    if count > 0:
        return count, "latest_unadjusted_daily_kline", _fmt_date(row.get("trade_date"))

    count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM si_all_code
        WHERE stock_code REGEXP '^(0|3|6)'
    """) or 0)
    return count, "active_stock_reference", ""


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

    expected_stocks, expected_source, expected_source_date = _expected_intraday_stock_count(engine)
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

    current_latest = _scalar(engine, "SELECT MAX(snapshot_at) FROM sm_stock_current")
    current_date = _fmt_date(current_latest)
    current_count_date = intraday_date
    current_non_trading_snapshot = False
    if current_date and current_date > intraday_date and not is_intraday_session(engine):
        current_count_date = current_date
        current_non_trading_snapshot = True
    current_day_start = datetime.strptime(current_count_date, "%Y-%m-%d")
    current_day_end = current_day_start + timedelta(days=1)
    current_row = _row(engine, """
        SELECT MAX(snapshot_at) AS latest_snapshot,
               COUNT(DISTINCT stock_code) AS stock_count,
               COUNT(*) AS row_count
        FROM sm_stock_current
        WHERE snapshot_at >= :day_start
          AND snapshot_at < :day_end
    """, {"day_start": current_day_start, "day_end": current_day_end})
    current_latest = current_row.get("latest_snapshot") or current_latest
    current_date = _fmt_date(current_latest)
    current_stocks = int(current_row.get("stock_count") or 0)
    current_rows = int(current_row.get("row_count") or 0)
    current_coverage = round(current_stocks / max(expected_stocks, 1), 4)

    flow_min_latest = _scalar(engine, "SELECT MAX(trade_time) FROM sm_stock_capital_flow_min")
    flow_min_date = _fmt_date(flow_min_latest)
    flow_day_start = (
        datetime.strptime(flow_min_date, "%Y-%m-%d")
        if flow_min_date
        else datetime.strptime(intraday_date, "%Y-%m-%d")
    )
    flow_day_end = flow_day_start + timedelta(days=1)
    flow_min_row = _row(engine, """
        SELECT MAX(trade_time) AS latest_time,
               COUNT(DISTINCT stock_code) AS stock_count,
               COUNT(*) AS row_count
        FROM sm_stock_capital_flow_min
        WHERE trade_time >= :day_start
          AND trade_time < :day_end
    """, {"day_start": flow_day_start, "day_end": flow_day_end})
    flow_min_date = _fmt_date(flow_min_row.get("latest_time")) or flow_min_date
    flow_min_stocks = int(flow_min_row.get("stock_count") or 0)
    flow_min_rows = int(flow_min_row.get("row_count") or 0)
    flow_min_coverage = round(flow_min_stocks / max(expected_stocks, 1), 4)

    snapshot_latest = _scalar(engine, "SELECT MAX(snapshot_at) FROM sm_rt_quote_snapshot")
    snapshot_date = _fmt_date(snapshot_latest)
    snapshot_count_date = intraday_date
    snapshot_non_trading_snapshot = False
    if snapshot_date and snapshot_date > intraday_date and not is_intraday_session(engine):
        snapshot_count_date = snapshot_date
        snapshot_non_trading_snapshot = True
    snapshot_day_start = datetime.strptime(snapshot_count_date, "%Y-%m-%d")
    snapshot_day_end = snapshot_day_start + timedelta(days=1)
    snapshot_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT stock_code)
        FROM sm_rt_quote_snapshot
        WHERE snapshot_at >= :day_start
          AND snapshot_at < :day_end
    """, {"day_start": snapshot_day_start, "day_end": snapshot_day_end}) or 0)
    snapshot_coverage = round(snapshot_count / max(expected_stocks, 1), 4)

    current_date_ok = current_date == intraday_date
    if not current_date_ok and current_non_trading_snapshot:
        current_date_ok = True

    snapshot_date_ok = snapshot_date == intraday_date
    if not snapshot_date_ok and snapshot_non_trading_snapshot:
        snapshot_date_ok = True

    ready = (
        expected_stocks > 0
        and minute_date == intraday_date
        and minute_coverage >= 0.70
        and current_date_ok
        and current_coverage >= 0.70
        and snapshot_date_ok
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
            "expected_stock_count_source": expected_source,
            "expected_stock_count_source_date": expected_source_date,
            "minute_date": minute_date,
            "minute_stock_count": minute_stocks,
            "minute_row_count": minute_rows,
            "minute_coverage": minute_coverage,
            "current_date": current_date,
            "current_date_ok": current_date_ok,
            "current_non_trading_snapshot": current_non_trading_snapshot,
            "current_stock_count": current_stocks,
            "stock_current_rows": current_rows,
            "current_coverage": current_coverage,
            "flow_min_date": flow_min_date,
            "flow_min_stock_count": flow_min_stocks,
            "flow_min_rows": flow_min_rows,
            "flow_min_coverage": flow_min_coverage,
            "snapshot_date": snapshot_date,
            "snapshot_date_ok": snapshot_date_ok,
            "snapshot_non_trading_snapshot": snapshot_non_trading_snapshot,
            "snapshot_stock_count": snapshot_count,
            "snapshot_coverage": snapshot_coverage,
        },
    )


def _legacy_check_news_and_notices(engine: Engine, trade_date: str) -> CheckResult:
    news_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM st_news_flash
        WHERE publish_time >= CONCAT(:d, ' 00:00:00')
    """, {"d": trade_date}) or 0)
    notice_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_notice_eastmoney
        WHERE notice_date BETWEEN DATE_SUB(:d, INTERVAL 7 DAY) AND :d
    """, {"d": trade_date}) or 0)
    ok = news_count >= 10 and notice_count >= 100
    warn = not ok and (news_count > 0 or notice_count > 0)
    return CheckResult(
        name="news_notice_freshness",
        status=_status(ok, warn),
        message=f"{trade_date} 新闻 {news_count} 条，近7日公告 {notice_count} 条",
        details={"trade_date": trade_date, "news_count": news_count, "notice_count_7d": notice_count},
    )


def _latest_day_count(
    engine: Engine,
    *,
    table: str,
    date_column: str,
    entity_column: str,
    predicate: str = "",
) -> dict[str, Any]:
    """Return latest business date and its coverage for a fixed internal table."""
    latest = _fmt_date(_scalar(
        engine,
        f"SELECT MAX({date_column}) FROM {table}" + (f" WHERE {predicate}" if predicate else ""),
    ))
    if not latest:
        return {"latest_date": "", "entity_count": 0, "row_count": 0}
    predicate_sql = f" AND {predicate}" if predicate else ""
    row = _row(engine, f"""
        SELECT COUNT(DISTINCT {entity_column}) AS entity_count, COUNT(*) AS row_count
        FROM {table}
        WHERE {date_column} = :latest_date{predicate_sql}
    """, {"latest_date": latest})
    return {
        "latest_date": latest,
        "entity_count": int(row.get("entity_count") or 0),
        "row_count": int(row.get("row_count") or 0),
    }


def check_index_data_freshness(
    engine: Engine,
    trade_date: str,
    now: datetime | None = None,
) -> CheckResult:
    tables = ("si_index_constituent", "sm_index_current", "sm_index_minute", "sm_index_kline")
    missing = [table for table in tables if not _table_exists(engine, table)]
    if missing:
        return CheckResult(
            "index_data_freshness",
            "FAIL",
            f"Missing index datasets: {', '.join(missing)}",
            {"missing": missing},
        )

    expected_current_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="18:30", now=now
    )
    expected_minute_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="16:30", now=now
    )
    expected_kline_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="16:25", now=now
    )
    constituent = _row(engine, """
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT index_code) AS index_count,
               MAX(etl_sync_at) AS latest_sync
        FROM si_index_constituent
    """)
    current = _latest_day_count(
        engine, table="sm_index_current", date_column="trade_date", entity_column="index_code"
    )
    minute = _latest_day_count(
        engine, table="sm_index_minute", date_column="trade_date", entity_column="index_code"
    )
    kline = _latest_day_count(
        engine,
        table="sm_index_kline",
        date_column="trade_date",
        entity_column="index_code",
        predicate="k_type = 1",
    )
    constituent_rows = int(constituent.get("row_count") or 0)
    constituent_indexes = int(constituent.get("index_count") or 0)
    min_constituent_rows = int(os.environ.get("DQ_INDEX_CONSTITUENT_MIN_ROWS", "1000"))
    min_index_count = int(os.environ.get("DQ_INDEX_MIN_COUNT", "100"))
    failures = []
    if constituent_rows < min_constituent_rows or constituent_indexes < 3:
        failures.append("index_constituent")
    if current["latest_date"] != expected_current_date or current["entity_count"] < min_index_count:
        failures.append("index_current")
    if minute["latest_date"] != expected_minute_date or minute["entity_count"] < min_index_count:
        failures.append("index_minute")
    if kline["latest_date"] != expected_kline_date or kline["entity_count"] < min_index_count:
        failures.append("index_kline")
    details = {
        "expected_dates": {
            "current": expected_current_date,
            "minute": expected_minute_date,
            "kline": expected_kline_date,
        },
        "constituent": {
            "row_count": constituent_rows,
            "index_count": constituent_indexes,
            "latest_sync": constituent.get("latest_sync"),
        },
        "current": current,
        "minute": minute,
        "kline": kline,
        "failures": failures,
    }
    return CheckResult(
        "index_data_freshness",
        _status(not failures),
        "Index datasets are fresh" if not failures else f"Stale or incomplete index datasets: {', '.join(failures)}",
        details,
    )


def check_concept_data_freshness(
    engine: Engine,
    trade_date: str,
    now: datetime | None = None,
) -> CheckResult:
    tables = (
        "si_concept_code_east",
        "si_concept_constituent_east",
        "sm_concept_east_current",
        "sm_concept_east_kline",
        "sm_concept_capital_flow_east",
    )
    missing = [table for table in tables if not _table_exists(engine, table)]
    if missing:
        return CheckResult(
            "concept_data_freshness",
            "FAIL",
            f"Missing concept datasets: {', '.join(missing)}",
            {"missing": missing, "source": "east"},
        )

    expected_current_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="16:00", now=now
    )
    expected_kline_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="16:10", now=now
    )
    expected_flow_date = expected_scheduled_trade_date(
        engine, trade_date, ready_time="19:45", now=now
    )
    reference = _row(engine, """
        SELECT
          (SELECT COUNT(DISTINCT concept_code) FROM si_concept_code_east) AS concept_count,
          (SELECT COUNT(*) FROM si_concept_constituent_east) AS constituent_count,
          GREATEST(
            COALESCE((SELECT MAX(etl_sync_at) FROM si_concept_code_east), '1970-01-01'),
            COALESCE((SELECT MAX(etl_sync_at) FROM si_concept_constituent_east), '1970-01-01')
          ) AS latest_sync
    """)
    current = _latest_day_count(
        engine, table="sm_concept_east_current", date_column="trade_date", entity_column="index_code"
    )
    kline = _latest_day_count(
        engine,
        table="sm_concept_east_kline",
        date_column="trade_date",
        entity_column="index_code",
        predicate="k_type = 1",
    )
    flow_latest = _fmt_date(_scalar(engine, "SELECT MAX(snapshot_at) FROM sm_concept_capital_flow_east"))
    flow_day = datetime.strptime(flow_latest or expected_flow_date, "%Y-%m-%d")
    flow_count = int(_scalar(engine, """
        SELECT COUNT(DISTINCT index_code)
        FROM sm_concept_capital_flow_east
        WHERE snapshot_at >= :day_start
          AND snapshot_at < :day_end
    """, {"day_start": flow_day, "day_end": flow_day + timedelta(days=1)}) or 0)
    concept_count = int(reference.get("concept_count") or 0)
    constituent_count = int(reference.get("constituent_count") or 0)
    min_concepts = int(os.environ.get("DQ_CONCEPT_MIN_COUNT", "50"))
    min_reference_coverage = max(
        0.0,
        min(1.0, float(os.environ.get("DQ_CONCEPT_COVERAGE_MIN", "0.80"))),
    )
    current_coverage = round(current["entity_count"] / max(concept_count, 1), 4)
    kline_coverage = round(kline["entity_count"] / max(concept_count, 1), 4)
    failures = []
    if concept_count < min_concepts or constituent_count < 500:
        failures.append("concept_reference")
    if (
        current["latest_date"] != expected_current_date
        or current["entity_count"] < min_concepts
        or current_coverage < min_reference_coverage
    ):
        failures.append("concept_current")
    if (
        kline["latest_date"] != expected_kline_date
        or kline["entity_count"] < min_concepts
        or kline_coverage < min_reference_coverage
    ):
        failures.append("concept_kline")
    if flow_latest != expected_flow_date or flow_count < min_concepts:
        failures.append("concept_flow")
    details = {
        "source": "east",
        "expected_dates": {
            "current": expected_current_date,
            "kline": expected_kline_date,
            "flow": expected_flow_date,
        },
        "reference": {
            "concept_count": concept_count,
            "constituent_count": constituent_count,
            "latest_sync": reference.get("latest_sync"),
        },
        "minimum_reference_coverage": min_reference_coverage,
        "current": {**current, "reference_coverage": current_coverage},
        "kline": {**kline, "reference_coverage": kline_coverage},
        "flow": {"latest_date": flow_latest, "concept_count": flow_count},
        "failures": failures,
    }
    return CheckResult(
        "concept_data_freshness",
        _status(not failures),
        "Concept datasets are fresh" if not failures else f"Stale or incomplete concept datasets: {', '.join(failures)}",
        details,
    )


def check_stock_snapshot_freshness(engine: Engine, trade_date: str) -> CheckResult:
    if not _table_exists(engine, "sm_stock_snapshot"):
        return CheckResult(
            "stock_snapshot_freshness",
            "FAIL",
            "Missing stock snapshot dataset: sm_stock_snapshot",
            {"missing": ["sm_stock_snapshot"]},
        )
    snapshot = _latest_day_count(
        engine, table="sm_stock_snapshot", date_column="trade_date", entity_column="stock_code"
    )
    min_count = int(os.environ.get("DQ_STOCK_SNAPSHOT_MIN_COUNT", "1000"))
    ok = snapshot["latest_date"] == trade_date and snapshot["entity_count"] >= min_count
    return CheckResult(
        "stock_snapshot_freshness",
        _status(ok),
        "Stock snapshot is fresh" if ok else "Stock snapshot is stale or incomplete",
        {"expected_date": trade_date, **snapshot, "minimum_stock_count": min_count},
    )


def check_news_and_notices(engine: Engine, trade_date: str) -> CheckResult:
    news_date = expected_intraday_date(engine, trade_date)
    news_day_start = datetime.strptime(news_date, "%Y-%m-%d")
    news_day_end = news_day_start + timedelta(days=1)
    news_row = _row(engine, """
        SELECT MAX(publish_time) AS latest_publish_time,
               SUM(CASE WHEN publish_time >= :day_start AND publish_time < :day_end THEN 1 ELSE 0 END) AS news_count
        FROM st_news_flash
    """, {"day_start": news_day_start, "day_end": news_day_end})
    news_count = int(news_row.get("news_count") or 0)
    latest_news_date = _fmt_date(news_row.get("latest_publish_time"))
    notice_count = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM si_notice_eastmoney
        WHERE notice_date BETWEEN DATE_SUB(:d, INTERVAL 7 DAY) AND :d
    """, {"d": trade_date}) or 0)
    ok = latest_news_date == news_date and news_count >= 10 and notice_count >= 100
    warn = not ok and (news_count > 0 or notice_count > 0)
    return CheckResult(
        name="news_notice_freshness",
        status=_status(ok, warn),
        message=f"{news_date} news {news_count}; recent notices {notice_count}; latest news {latest_news_date or '-'}",
        details={
            "trade_date": trade_date,
            "expected_news_date": news_date,
            "latest_news_date": latest_news_date,
            "latest_publish_time": news_row.get("latest_publish_time"),
            "news_count": news_count,
            "notice_count_7d": notice_count,
        },
    )


def check_analysis_outputs(engine: Engine, trade_date: str) -> CheckResult:
    analysis = _row(engine, """
        SELECT analysis_date,
               COUNT(*) AS analysis_count,
               SUM(CASE WHEN recommend_status IS NOT NULL
                              AND TRIM(recommend_status) <> '' THEN 1 ELSE 0 END) AS status_count,
               SUM(CASE WHEN UPPER(TRIM(recommend_status)) = 'ALLOW' THEN 1 ELSE 0 END) AS allow_count,
               (SELECT COUNT(DISTINCT stock_code)
                FROM sm_stock_kline
                WHERE trade_date = :trade_date AND k_type = 1 AND adjust_type = 0) AS expected_count
        FROM stock_analysis_result
        WHERE analysis_date = (SELECT MAX(analysis_date) FROM stock_analysis_result)
        GROUP BY analysis_date
    """, {"trade_date": trade_date})
    analysis_date = _fmt_date(analysis.get("analysis_date"))
    analysis_count = int(analysis.get("analysis_count") or 0)
    status_count = int(analysis.get("status_count") or 0)
    allow_count = int(analysis.get("allow_count") or 0)
    expected_analysis_count = int(analysis.get("expected_count") or 0)
    analysis_coverage = round(
        analysis_count / max(expected_analysis_count, 1),
        4,
    )
    minimum_analysis_coverage = max(
        0.0,
        min(1.0, float(os.environ.get("DQ_ANALYSIS_COVERAGE_MIN", "0.80"))),
    )

    recommendation = _row(engine, """
        SELECT
          (SELECT MAX(pick_date) FROM st_recommended_stocks) AS latest_date,
          (SELECT COUNT(*) FROM st_recommended_stocks WHERE pick_date = :trade_date) AS current_count,
          (SELECT COUNT(*) FROM st_recommended_stocks
           WHERE pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks)) AS latest_count
    """, {"trade_date": trade_date})
    latest_rec_date = _fmt_date(recommendation.get("latest_date"))
    latest_rec_count = int(recommendation.get("latest_count") or 0)
    current_rec_count = int(recommendation.get("current_count") or 0)

    run = {}
    if _table_exists(engine, "st_recommended_run_history"):
        run = _row(engine, """
            SELECT status, total, passed, started_at, finished_at
            FROM st_recommended_run_history
            WHERE trade_date = :trade_date
            ORDER BY COALESCE(finished_at, started_at, created_at) DESC, id DESC
            LIMIT 1
        """, {"trade_date": trade_date})

    run_status = str(run.get("status") or "").strip().lower()
    try:
        run_total = int(run.get("total") or 0)
    except (TypeError, ValueError):
        run_total = 0
    run_completed = (
        run_status in {"done", "success", "completed"}
        and bool(run.get("finished_at"))
        and run_total >= 1000
        and (
            expected_analysis_count <= 0
            or run_total / expected_analysis_count >= minimum_analysis_coverage
        )
    )
    state = "not_confirmed"
    evidence = ""
    if current_rec_count > 0:
        state = "current_pool"
        evidence = "st_recommended_stocks"
    elif run_completed and run.get("passed") is not None:
        if int(run.get("passed") or 0) == 0:
            state = "completed_zero"
            evidence = "st_recommended_run_history"
        else:
            state = "inconsistent_completed_run"
            evidence = "st_recommended_run_history"
    elif run_status in {"done", "success", "completed"}:
        state = "incomplete_run_history"
        evidence = "st_recommended_run_history"
    elif run_status in {"queued", "running"}:
        state = "running"
        evidence = "st_recommended_run_history"
    elif run_status in {"error", "failed", "timeout", "stopped"}:
        state = "failed"
        evidence = "st_recommended_run_history"
    elif (
        analysis_date == trade_date
        and analysis_count >= 1000
        and expected_analysis_count >= 1000
        and analysis_coverage >= minimum_analysis_coverage
        and status_count == analysis_count
        and allow_count == 0
    ):
        # ``sync_analysis_fast.save_outputs`` writes analysis statuses and the
        # same-day recommendation set in one transaction.  A complete current
        # analysis with no ALLOW status is therefore positive evidence of a
        # successful zero-candidate result, not a stale recommendation pool.
        state = "completed_zero"
        evidence = "stock_analysis_result.recommend_status"

    ok = analysis_count >= 1000
    recommendation_complete = state in {"current_pool", "completed_zero"}
    warn = ok and (analysis_date != trade_date or not recommendation_complete)
    if state == "current_pool":
        recommendation_message = f"推荐池 {trade_date}: {current_rec_count} 条"
    elif state == "completed_zero":
        recommendation_message = f"推荐 {trade_date} 已运行，0 候选"
    elif state == "running":
        recommendation_message = f"推荐 {trade_date} 仍在运行"
    elif state == "failed":
        recommendation_message = f"推荐 {trade_date} 运行失败"
    elif state in {"inconsistent_completed_run", "incomplete_run_history"}:
        recommendation_message = f"推荐 {trade_date} 运行记录与结果表不一致"
    else:
        recommendation_message = (
            f"推荐 {trade_date} 尚无完成证据；历史池 "
            f"{latest_rec_date or '-'}: {latest_rec_count} 条"
        )
    return CheckResult(
        name="analysis_outputs",
        status=_status(ok, warn),
        message=(
            f"分析结果 {analysis_date or '-'}: {analysis_count} 条；"
            f"{recommendation_message}"
        ),
        details={
            "trade_date": trade_date,
            "analysis_date": analysis_date,
            "analysis_count": analysis_count,
            "expected_analysis_count": expected_analysis_count,
            "analysis_coverage": analysis_coverage,
            "minimum_analysis_coverage": minimum_analysis_coverage,
            "analysis_recommend_status_count": status_count,
            "analysis_allow_count": allow_count,
            "recommend_date": trade_date if recommendation_complete else latest_rec_date,
            "recommend_count": current_rec_count if recommendation_complete else latest_rec_count,
            "recommendation_state": state,
            "recommendation_evidence": evidence,
            "latest_stored_recommend_date": latest_rec_date,
            "latest_stored_recommend_count": latest_rec_count,
            "run_history": run,
        },
    )


def _scheduler_health_status(bad_tasks: list[dict[str, Any]]) -> str:
    """Scheduler issues are operational warnings unless the task table is missing."""
    return "WARN" if bad_tasks else "PASS"


INTRADAY_SCHEDULER_TASK_TYPES = {
    "intraday_realtime",
    "qmt_intraday_realtime",
    "intraday_minute_kline",
    "intraday_minute_flow",
    "intraday_quality_check",
    "sim_trade",
    "sim_trade_signal_prepare",
}

SELF_MONITOR_TASK_TYPES = {
    "quality_check_pre",
    "quality_check_post",
    "intraday_quality_check",
}

LONG_RUNNING_TASK_TYPES = {
    "qmt_local_history_2024",
}


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None or value == "":
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def _cron_minutes(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        hour, minute = (int(part) for part in raw[:5].split(":", 1))
    except (TypeError, ValueError):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour * 60 + minute


def _scheduler_bad_tasks(
    engine: Engine,
    *,
    task_types: set[str] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    filter_sql = ""
    params: dict[str, Any] = {}
    if task_types:
        placeholders = []
        for idx, task_type in enumerate(sorted(task_types)):
            key = f"task_type_{idx}"
            placeholders.append(f":{key}")
            params[key] = task_type
        filter_sql = " AND task_type IN (" + ", ".join(placeholders) + ")"
    excluded_placeholders = []
    for idx, task_type in enumerate(sorted(SELF_MONITOR_TASK_TYPES)):
        key = f"excluded_task_type_{idx}"
        excluded_placeholders.append(f":{key}")
        params[key] = task_type
    rows = _rows(engine, f"""
        SELECT task_name, task_type, script_path, cron_time, interval_minutes,
               last_run_status, last_run_at, last_triggered_at, last_run_output,
               TIMESTAMPDIFF(MINUTE, last_run_at, NOW()) AS age_minutes
        FROM st_scheduled_tasks
        WHERE enabled = 1
          {filter_sql}
          AND task_type NOT IN ({", ".join(excluded_placeholders)})
        ORDER BY task_name
    """, params)
    if not rows:
        return []

    now = now or datetime.now()
    in_intraday_session = is_intraday_session(engine, now)
    cron_grace = max(0, int(os.environ.get("DQ_SCHEDULER_CRON_GRACE_MINUTES", "15")))
    stale_running = max(1, int(os.environ.get("DQ_SCHEDULER_RUNNING_STALE_MINUTES", "30")))
    bad: list[dict[str, Any]] = []

    for source in rows:
        row = dict(source)
        task_type = str(row.get("task_type") or "")
        status = str(row.get("last_run_status") or "").strip().lower()
        output = str(row.get("last_run_output") or "")
        last_run = _coerce_datetime(row.get("last_run_at"))
        last_triggered = _coerce_datetime(row.get("last_triggered_at"))
        activity_values = [value for value in (last_run, last_triggered) if value is not None]
        last_activity = max(activity_values) if activity_values else None
        age_minutes = int((now - last_activity).total_seconds() // 60) if last_activity else None
        triggered_today = bool(last_triggered and last_triggered.date() == now.date())
        ran_today = bool(last_run and last_run.date() == now.date())
        if '"reason": "market_closed"' in output and ran_today:
            continue

        try:
            interval = max(0, int(row.get("interval_minutes") or 0))
        except (TypeError, ValueError):
            interval = 0
        is_intraday = task_type in INTRADAY_SCHEDULER_TASK_TYPES
        cron = _cron_minutes(row.get("cron_time"))
        scheduled_at = None
        cron_due = False
        if cron is not None:
            scheduled_at = now.replace(hour=cron // 60, minute=cron % 60, second=0, microsecond=0)
            cron_due = now >= scheduled_at + timedelta(minutes=cron_grace)
        interval_active = interval > 0 and (not is_intraday or in_intraday_session)

        issue = ""
        expected_by: datetime | None = None
        if status == "running":
            if task_type not in LONG_RUNNING_TASK_TYPES and (
                last_activity is None or age_minutes is None or age_minutes > stale_running
            ):
                issue = "stale_running"
                expected_by = now - timedelta(minutes=stale_running)
        elif status in {"failed", "timeout", "stopped"}:
            if interval > 0:
                if interval_active or triggered_today or ran_today:
                    issue = f"status_{status}"
            elif triggered_today or ran_today or cron_due or cron is None:
                issue = f"status_{status}"
        elif not status or (status == "success" and last_run is None):
            if interval_active or cron_due:
                issue = "never_completed"
                expected_by = scheduled_at
        elif status == "success":
            if interval_active:
                interval_grace = max(5, interval * 3)
                expected_by = now - timedelta(minutes=interval_grace)
                if last_activity is None or last_activity < expected_by:
                    issue = "overdue_interval"
            elif cron_due and scheduled_at is not None:
                expected_by = scheduled_at + timedelta(minutes=cron_grace)
                if last_run is None or last_run < scheduled_at:
                    issue = "overdue_cron"

        if not issue:
            continue
        # Scheduler output may contain very large logs and connector URLs.
        # It is needed for classification above but must not leak into health
        # report payloads.
        row.pop("last_run_output", None)
        row["issue"] = issue
        row["age_minutes"] = age_minutes
        row["expected_by"] = expected_by
        bad.append(row)

    limit = max(1, int(os.environ.get("DQ_SCHEDULER_BAD_TASK_LIMIT", "50")))
    return bad[:limit]


def check_scheduler_health(engine: Engine) -> CheckResult:
    if not _table_exists(engine, "st_scheduled_tasks"):
        return CheckResult("scheduler_health", "WARN", "缺少调度任务表")
    bad = _scheduler_bad_tasks(engine)
    status = _scheduler_health_status(bad)
    return CheckResult(
        name="scheduler_health",
        status=status,
        message="调度任务状态正常" if not bad else f"调度异常任务 {len(bad)} 个",
        details={"bad_tasks": bad},
    )


def check_intraday_scheduler_health(engine: Engine) -> CheckResult:
    if not _table_exists(engine, "st_scheduled_tasks"):
        return CheckResult("intraday_scheduler_health", "WARN", "缺少调度任务表")
    bad = _scheduler_bad_tasks(engine, task_types=INTRADAY_SCHEDULER_TASK_TYPES)
    status = _scheduler_health_status(bad)
    return CheckResult(
        name="intraday_scheduler_health",
        status=status,
        message="盘中调度状态正常" if not bad else f"盘中调度异常任务 {len(bad)} 个",
        details={"bad_tasks": bad, "task_types": sorted(INTRADAY_SCHEDULER_TASK_TYPES)},
    )


def check_schema_collation(engine: Engine) -> CheckResult:
    targets = [
        "si_all_code.stock_code",
        "sm_stock_kline.stock_code",
        "sm_stock_capital_flow_daily.stock_code",
        "st_user_portfolio.stock_code",
        "sm_stock_current.stock_code",
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
        WHERE COALESCE(trade_mode, '') NOT IN (
            'live', 'backtest', 'forward', 'invalid_offhours', 'manual_bookkeeping'
        )
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
        check_recent_kline_calendar_completeness(engine, trade_date),
        check_flow_coverage(engine, trade_date),
        check_index_data_freshness(engine, trade_date),
        check_concept_data_freshness(engine, trade_date),
        check_stock_snapshot_freshness(engine, trade_date),
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
    parser.add_argument("--acquisition", action="store_true", help="只读汇总采集日历、两端心跳、近期漏日及当日资金流；不依赖策略结果")
    args = parser.parse_args()

    if args.acquisition and (args.readiness or args.skip_closed):
        parser.error("--acquisition 不能与 --readiness/--skip-closed 同用；闭市后仍须检查漏数")

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

    report = (run_acquisition_checks(engine, args.date.strip() or None) if args.acquisition
              else run_checks(engine, args.date.strip() or None, include_realtime=args.include_realtime))
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
