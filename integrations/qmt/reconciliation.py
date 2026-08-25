from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt.audit import validate_audit_schema
from integrations.qmt.diagnostics import PROVIDER_ID
from integrations.qmt.pending_write import replay_pending_writes, result_dict as pending_result_dict
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt


CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")


@dataclass(frozen=True)
class CoverageResult:
    dataset: str
    trade_date: date
    expected_count: int
    actual_count: int
    missing_count: int
    coverage_ratio: float
    status: str
    details: dict[str, Any]


@dataclass(frozen=True)
class QualityResult:
    dataset: str
    rule_name: str
    status: str
    checked_rows: int
    failed_rows: int
    metric_value: float | None = None
    threshold_value: float | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class NightlyReconciliationResult:
    run_id: str
    status: str
    target_trade_date: str | None
    scan_days: int
    pending_replay: dict[str, Any]
    coverage: list[dict[str, Any]]
    quality: list[dict[str, Any]]
    gaps_created_or_open: int
    started_at: str
    finished_at: str
    error_message: str | None = None


def _now() -> datetime:
    return datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def _table_exists(conn, table_name: str) -> bool:
    value = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            """
        ),
        {"table_name": table_name},
    ).scalar()
    return bool(value)


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return {str(row[0]) for row in rows}


def _previous_trade_dates(engine: Engine, *, scan_days: int, today: date | None = None) -> list[date]:
    today_value = today or _now().date()
    lookback_start = today_value - timedelta(days=max(60, scan_days * 4))
    with engine.begin() as conn:
        receipt = load_trade_calendar_receipt(
            conn,
            start_date=lookback_start.isoformat(),
            end_date=today_value.isoformat(),
            decision_known_at=_now().replace(tzinfo=None, microsecond=0),
        )
    sessions = [
        datetime.strptime(day, "%Y-%m-%d").date()
        for day in receipt.sessions_between(
            lookback_start.isoformat(), today_value.isoformat()
        )
        if day < today_value.isoformat()
    ]
    if len(sessions) < max(1, int(scan_days)):
        raise RuntimeError("immutable QMT calendar receipt has too few sessions")
    return list(reversed(sessions[-max(1, int(scan_days)):]))


def _expected_stock_sets(
    engine: Engine, dates: Sequence[date],
) -> tuple[Any, Any, dict[date, set[str]]]:
    if not dates:
        raise RuntimeError("QMT reconciliation requires target sessions")
    with engine.begin() as conn:
        catalog = load_stock_catalog(
            conn,
            decision_known_at=_now().replace(tzinfo=None, microsecond=0),
        )
        calendar_receipt = load_trade_calendar_receipt(
            conn,
            start_date=min(dates).isoformat(),
            end_date=max(dates).isoformat(),
            decision_known_at=_now().replace(tzinfo=None, microsecond=0),
        )
    expected = {
        trade_date: set(catalog.eligible_codes(trade_date.isoformat()))
        for trade_date in dates
    }
    if any(not codes for codes in expected.values()):
        raise RuntimeError("independent QMT reconciliation universe is empty")
    if set(calendar_receipt.sessions_between(
        min(dates).isoformat(), max(dates).isoformat()
    )) != {trade_date.isoformat() for trade_date in dates}:
        raise RuntimeError("QMT reconciliation dates differ from calendar root")
    return catalog, calendar_receipt, expected


def _group_counts_for_dates(engine: Engine, table_name: str, dates: Sequence[date]) -> dict[date, int]:
    if not dates:
        return {}
    placeholders = ", ".join(f":d{idx}" for idx, _ in enumerate(dates))
    params = {f"d{idx}": item for idx, item in enumerate(dates)}
    with engine.begin() as conn:
        if not _table_exists(conn, table_name):
            return {}
        rows = conn.execute(
            text(
                f"""
                SELECT trade_date, COUNT(DISTINCT stock_code) AS cnt
                FROM `{table_name}`
                WHERE trade_date IN ({placeholders})
                GROUP BY trade_date
                """
            ),
            params,
        ).fetchall()
    return {row[0]: int(row[1] or 0) for row in rows}


def _group_stock_sets_for_dates(
    engine: Engine, table_name: str, dates: Sequence[date],
) -> dict[date, set[str]]:
    if not dates:
        return {}
    placeholders = ", ".join(f":d{idx}" for idx, _ in enumerate(dates))
    params = {f"d{idx}": item for idx, item in enumerate(dates)}
    filters = ""
    if table_name == "sm_stock_kline":
        filters = " AND k_type=1 AND adjust_type=0"
    with engine.begin() as conn:
        if not _table_exists(conn, table_name):
            return {}
        rows = conn.execute(text(f"""
            SELECT DISTINCT trade_date, stock_code
            FROM `{table_name}`
            WHERE trade_date IN ({placeholders})
              AND stock_code REGEXP '^(0|3|4|6|8|9)'
              {filters}
            ORDER BY trade_date, stock_code
        """), params).fetchall()
    result: dict[date, set[str]] = {}
    for trade_date, stock_code in rows:
        result.setdefault(trade_date, set()).add(
            str(stock_code).strip().zfill(6)
        )
    return result


def _coverage_status(ratio: float, *, warn_threshold: float, pass_threshold: float) -> str:
    if ratio >= pass_threshold:
        return "PASS"
    if ratio >= warn_threshold:
        return "WARN"
    return "FAIL"


def _price_consistency_status(*, checked_rows: int, failed_rows: int) -> str:
    if checked_rows <= 0:
        return "WARN"
    if failed_rows > 0:
        return "FAIL"
    return "PASS"


def _missing_quality_result(dataset: str, rule_name: str, reason: str) -> QualityResult:
    return QualityResult(
        dataset=dataset,
        rule_name=rule_name,
        status="WARN",
        checked_rows=0,
        failed_rows=0,
        metric_value=None,
        threshold_value=None,
        details={"reason": reason},
    )


def build_coverage_results(engine: Engine, *, scan_days: int) -> list[CoverageResult]:
    dates = _previous_trade_dates(engine, scan_days=scan_days)
    catalog, calendar_receipt, expected = _expected_stock_sets(engine, dates)
    daily_sets = _group_stock_sets_for_dates(engine, "sm_stock_kline", dates)
    minute_sets = _group_stock_sets_for_dates(engine, "sm_stock_minute", dates)

    results: list[CoverageResult] = []
    for trade_date in dates:
        expected_set = expected.get(trade_date) or set()
        expected_count = len(expected_set)
        for dataset, actual_sets, warn, passed in (
            ("sm_stock_kline.1d", daily_sets, 1.0, 1.0),
            ("sm_stock_minute.1m", minute_sets, 0.50, 0.80),
        ):
            actual_set = actual_sets.get(trade_date) or set()
            missing = sorted(expected_set - actual_set)
            unexpected = sorted(actual_set - expected_set)
            actual_count = len(actual_set)
            missing_count = len(missing)
            matched_count = len(expected_set & actual_set)
            ratio = (matched_count / expected_count) if expected_count else 0.0
            if dataset == "sm_stock_kline.1d":
                status = "PASS" if not missing and not unexpected else "FAIL"
            else:
                status = _coverage_status(
                    ratio, warn_threshold=warn, pass_threshold=passed
                )
            results.append(
                CoverageResult(
                    dataset=dataset,
                    trade_date=trade_date,
                    expected_count=expected_count,
                    actual_count=actual_count,
                    missing_count=missing_count,
                    coverage_ratio=round(ratio, 8),
                    status=status,
                    details={
                        "warn_threshold": warn,
                        "pass_threshold": passed,
                        "exact_set_required": dataset == "sm_stock_kline.1d",
                        "missing_count": len(missing),
                        "missing_sample": missing[:20],
                        "unexpected_count": len(unexpected),
                        "unexpected_sample": unexpected[:20],
                        "catalog_batch_id": catalog.batch_id,
                        "catalog_member_set_hash": catalog.member_set_hash,
                        "catalog_manifest_hash": catalog.manifest_hash,
                        "calendar_batch_id": calendar_receipt.batch_id,
                        "calendar_session_set_hash": (
                            calendar_receipt.session_set_hash
                        ),
                        "calendar_manifest_hash": calendar_receipt.manifest_hash,
                        "calendar_known_at": calendar_receipt.known_at,
                        "history_backfill": "deferred_queue",
                    },
                )
            )
    return results


def _daily_minute_close_consistency(conn, *, target_trade_date: date) -> QualityResult:
    rule_name = "daily_minute_close_consistency"
    if not _table_exists(conn, "sm_stock_kline") or not _table_exists(conn, "sm_stock_minute"):
        return _missing_quality_result("sm_stock_minute.1m", rule_name, "required_table_missing")

    daily_columns = _table_columns(conn, "sm_stock_kline")
    minute_columns = _table_columns(conn, "sm_stock_minute")
    required_daily = {"stock_code", "trade_date", "k_type", "close"}
    required_minute = {"stock_code", "trade_date", "trade_time", "price"}
    if not required_daily.issubset(daily_columns) or not required_minute.issubset(minute_columns):
        return _missing_quality_result(
            "sm_stock_minute.1m",
            rule_name,
            "required_columns_missing",
        )

    daily_source_filter = "AND k.data_source = :provider" if "data_source" in daily_columns else ""
    minute_latest_source_filter = "AND data_source = :provider" if "data_source" in minute_columns else ""
    minute_source_filter = "AND mm.data_source = :provider" if "data_source" in minute_columns else ""
    tolerance_ratio = 0.005
    min_abs_diff = 0.01
    row = conn.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS checked_rows,
                COALESCE(SUM(
                    CASE
                        WHEN ABS(q.minute_price - q.daily_close)
                             > GREATEST(:min_abs_diff, ABS(q.daily_close) * :tolerance_ratio)
                        THEN 1 ELSE 0
                    END
                ), 0) AS failed_rows,
                COALESCE(MAX(ABS(q.minute_price - q.daily_close) / NULLIF(ABS(q.daily_close), 0)), 0) AS max_ratio,
                COALESCE(MAX(ABS(q.minute_price - q.daily_close)), 0) AS max_abs_diff
            FROM (
                SELECT k.stock_code, k.close AS daily_close, m.price AS minute_price
                FROM sm_stock_kline k
                JOIN (
                    SELECT mm.stock_code, mm.price, mm.trade_time
                    FROM sm_stock_minute mm
                    JOIN (
                        SELECT stock_code, MAX(trade_time) AS max_trade_time
                        FROM sm_stock_minute
                        WHERE trade_date = :trade_date
                          {minute_latest_source_filter}
                        GROUP BY stock_code
                    ) latest
                      ON latest.stock_code = mm.stock_code
                     AND latest.max_trade_time = mm.trade_time
                    WHERE mm.trade_date = :trade_date
                      AND mm.price IS NOT NULL
                      {minute_source_filter}
                ) m ON m.stock_code = k.stock_code
                WHERE k.trade_date = :trade_date
                  AND k.k_type = 1
                  AND k.close IS NOT NULL
                  AND k.close > 0
                  {daily_source_filter}
            ) q
            """
        ),
        {
            "trade_date": target_trade_date,
            "provider": PROVIDER_ID,
            "tolerance_ratio": tolerance_ratio,
            "min_abs_diff": min_abs_diff,
        },
    ).mappings().first()
    checked_rows = int((row or {}).get("checked_rows") or 0)
    failed_rows = int((row or {}).get("failed_rows") or 0)
    max_ratio = float((row or {}).get("max_ratio") or 0.0)
    max_abs = float((row or {}).get("max_abs_diff") or 0.0)
    return QualityResult(
        dataset="sm_stock_minute.1m",
        rule_name=rule_name,
        status=_price_consistency_status(checked_rows=checked_rows, failed_rows=failed_rows),
        checked_rows=checked_rows,
        failed_rows=failed_rows,
        metric_value=round(max_ratio, 8),
        threshold_value=tolerance_ratio,
        details={
            "trade_date": target_trade_date.isoformat(),
            "compare": "latest_minute_price_vs_daily_close",
            "min_abs_diff": min_abs_diff,
            "max_abs_diff": round(max_abs, 6),
            "qmt_provenance_filter": {
                "daily": bool(daily_source_filter),
                "minute": bool(minute_source_filter),
            },
        },
    )


def _current_daily_close_consistency(conn, *, target_trade_date: date) -> QualityResult:
    rule_name = "current_daily_close_consistency"
    if not _table_exists(conn, "sm_stock_current") or not _table_exists(conn, "sm_stock_kline"):
        return _missing_quality_result("sm_stock_current", rule_name, "required_table_missing")

    current_columns = _table_columns(conn, "sm_stock_current")
    daily_columns = _table_columns(conn, "sm_stock_kline")
    if not {"stock_code", "price"}.issubset(current_columns) or not {"stock_code", "trade_date", "k_type", "close"}.issubset(daily_columns):
        return _missing_quality_result("sm_stock_current", rule_name, "required_columns_missing")

    date_expr = ""
    if "trade_date" in current_columns:
        date_expr = "c.trade_date"
    else:
        for column in ("trade_time", "snapshot_at", "source_time", "received_at"):
            if column in current_columns:
                date_expr = f"DATE(c.`{column}`)"
                break
    if not date_expr:
        return _missing_quality_result("sm_stock_current", rule_name, "current_time_column_missing")

    current_source_filter = "AND c.data_source = :provider" if "data_source" in current_columns else ""
    daily_source_filter = "AND k.data_source = :provider" if "data_source" in daily_columns else ""
    tolerance_ratio = 0.005
    min_abs_diff = 0.01
    row = conn.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS checked_rows,
                COALESCE(SUM(
                    CASE
                        WHEN ABS(c.price - k.close)
                             > GREATEST(:min_abs_diff, ABS(k.close) * :tolerance_ratio)
                        THEN 1 ELSE 0
                    END
                ), 0) AS failed_rows,
                COALESCE(MAX(ABS(c.price - k.close) / NULLIF(ABS(k.close), 0)), 0) AS max_ratio,
                COALESCE(MAX(ABS(c.price - k.close)), 0) AS max_abs_diff
            FROM sm_stock_current c
            JOIN sm_stock_kline k
              ON k.stock_code = c.stock_code
             AND k.trade_date = :trade_date
             AND k.k_type = 1
            WHERE {date_expr} = :trade_date
              AND c.price IS NOT NULL
              AND k.close IS NOT NULL
              AND k.close > 0
              {current_source_filter}
              {daily_source_filter}
            """
        ),
        {
            "trade_date": target_trade_date,
            "provider": PROVIDER_ID,
            "tolerance_ratio": tolerance_ratio,
            "min_abs_diff": min_abs_diff,
        },
    ).mappings().first()
    checked_rows = int((row or {}).get("checked_rows") or 0)
    failed_rows = int((row or {}).get("failed_rows") or 0)
    max_ratio = float((row or {}).get("max_ratio") or 0.0)
    max_abs = float((row or {}).get("max_abs_diff") or 0.0)
    return QualityResult(
        dataset="sm_stock_current",
        rule_name=rule_name,
        status=_price_consistency_status(checked_rows=checked_rows, failed_rows=failed_rows),
        checked_rows=checked_rows,
        failed_rows=failed_rows,
        metric_value=round(max_ratio, 8),
        threshold_value=tolerance_ratio,
        details={
            "trade_date": target_trade_date.isoformat(),
            "compare": "current_price_vs_daily_close",
            "current_date_expression": date_expr,
            "min_abs_diff": min_abs_diff,
            "max_abs_diff": round(max_abs, 6),
            "qmt_provenance_filter": {
                "current": bool(current_source_filter),
                "daily": bool(daily_source_filter),
            },
        },
    )


def build_quality_results(engine: Engine, *, target_trade_date: date | None) -> list[QualityResult]:
    if target_trade_date is None:
        return []
    with engine.begin() as conn:
        current_duplicates = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT stock_code
                        FROM sm_stock_current
                        GROUP BY stock_code
                        HAVING COUNT(*) > 1
                    ) q
                    """
                )
            ).scalar()
            or 0
        )
        daily_total = int(
            conn.execute(
                text("SELECT COUNT(*) FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1"),
                {"d": target_trade_date},
            ).scalar()
            or 0
        )
        daily_ohlc_failed = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM sm_stock_kline
                    WHERE trade_date = :d
                      AND k_type = 1
                      AND (
                        open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL
                        OR high < open OR high < close OR high < low
                        OR low > open OR low > close OR low > high
                        OR COALESCE(volume, 0) < 0 OR COALESCE(amount, 0) < 0
                      )
                    """
                ),
                {"d": target_trade_date},
            ).scalar()
            or 0
        )

        results = [
            QualityResult(
                dataset="sm_stock_current",
                rule_name="unique_stock_code",
                status="PASS" if current_duplicates == 0 else "FAIL",
                checked_rows=current_duplicates,
                failed_rows=current_duplicates,
                metric_value=float(current_duplicates),
                threshold_value=0.0,
                details={"index_required": "uk_qmt_sm_stock_current_code"},
            ),
            QualityResult(
                dataset="sm_stock_kline.1d",
                rule_name="ohlc_volume_amount_basic",
                status="PASS" if daily_ohlc_failed == 0 else "FAIL",
                checked_rows=daily_total,
                failed_rows=daily_ohlc_failed,
                metric_value=float(daily_ohlc_failed),
                threshold_value=0.0,
                details={"trade_date": target_trade_date.isoformat()},
            ),
        ]
        results.append(_daily_minute_close_consistency(conn, target_trade_date=target_trade_date))
        results.append(_current_daily_close_consistency(conn, target_trade_date=target_trade_date))
    return results


def _insert_sync_run_start(conn, *, run_id: str, target_trade_date: date | None, started_at: datetime) -> None:
    conn.execute(
        text(
            """
            INSERT INTO sys_data_sync_run (
                run_id, provider, task_type, target_trade_date, status,
                started_at, extra_json
            ) VALUES (
                :run_id, :provider, 'nightly_data_reconciliation', :target_trade_date,
                'RUNNING', :started_at, :extra_json
            )
            ON DUPLICATE KEY UPDATE
                status = 'RUNNING',
                started_at = VALUES(started_at),
                finished_at = NULL,
                error_message = NULL,
                extra_json = VALUES(extra_json)
            """
        ),
        {
            "run_id": run_id,
            "provider": PROVIDER_ID,
            "target_trade_date": target_trade_date,
            "started_at": started_at,
            "extra_json": _json({"version": 1}),
        },
    )


def _finish_sync_run(
    conn,
    *,
    run_id: str,
    status: str,
    finished_at: datetime,
    expected_count: int,
    actual_count: int,
    missing_count: int,
    error_message: str | None,
    extra: dict[str, Any],
) -> None:
    conn.execute(
        text(
            """
            UPDATE sys_data_sync_run
            SET status = :status,
                expected_count = :expected_count,
                actual_count = :actual_count,
                missing_count = :missing_count,
                finished_at = :finished_at,
                error_message = :error_message,
                extra_json = :extra_json
            WHERE run_id = :run_id
            """
        ),
        {
            "run_id": run_id,
            "status": status,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "missing_count": missing_count,
            "finished_at": finished_at,
            "error_message": error_message,
            "extra_json": _json(extra),
        },
    )


def _save_coverage(conn, *, run_id: str, coverage: Iterable[CoverageResult], checked_at: datetime) -> int:
    count = 0
    for item in coverage:
        count += 1
        conn.execute(
            text(
                """
                INSERT INTO sys_data_coverage (
                    provider, dataset, trade_date, expected_count, actual_count,
                    missing_count, coverage_ratio, status, batch_id, details_json, checked_at
                ) VALUES (
                    :provider, :dataset, :trade_date, :expected_count, :actual_count,
                    :missing_count, :coverage_ratio, :status, :batch_id, :details_json, :checked_at
                )
                ON DUPLICATE KEY UPDATE
                    expected_count = VALUES(expected_count),
                    actual_count = VALUES(actual_count),
                    missing_count = VALUES(missing_count),
                    coverage_ratio = VALUES(coverage_ratio),
                    status = VALUES(status),
                    batch_id = VALUES(batch_id),
                    details_json = VALUES(details_json),
                    checked_at = VALUES(checked_at)
                """
            ),
            {
                "provider": PROVIDER_ID,
                "dataset": item.dataset,
                "trade_date": item.trade_date,
                "expected_count": item.expected_count,
                "actual_count": item.actual_count,
                "missing_count": item.missing_count,
                "coverage_ratio": item.coverage_ratio,
                "status": item.status,
                "batch_id": run_id,
                "details_json": _json(item.details),
                "checked_at": checked_at,
            },
        )
    return count


def _gap_exists(conn, item: CoverageResult) -> bool:
    value = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM sys_data_gap
            WHERE provider = :provider
              AND dataset = :dataset
              AND symbol = ''
              AND period = :period
              AND gap_start = :gap_start
              AND gap_end = :gap_end
              AND status IN ('PENDING', 'RETRYING')
            """
        ),
        {
            "provider": PROVIDER_ID,
            "dataset": item.dataset,
            "period": item.dataset.rsplit(".", 1)[-1] if "." in item.dataset else "",
            "gap_start": datetime.combine(item.trade_date, datetime.min.time()),
            "gap_end": datetime.combine(item.trade_date, datetime.max.time()).replace(microsecond=0),
        },
    ).scalar()
    return bool(value)


def _save_gaps(conn, *, run_id: str, coverage: Iterable[CoverageResult], checked_at: datetime) -> int:
    created_or_open = 0
    for item in coverage:
        gap_start = datetime.combine(item.trade_date, datetime.min.time())
        gap_end = datetime.combine(item.trade_date, datetime.max.time()).replace(microsecond=0)
        period = item.dataset.rsplit(".", 1)[-1] if "." in item.dataset else ""
        if item.status == "PASS":
            conn.execute(
                text(
                    """
                    UPDATE sys_data_gap
                    SET status = 'RESOLVED', resolved_at = :resolved_at, updated_at = :updated_at
                    WHERE provider = :provider
                      AND dataset = :dataset
                      AND symbol = ''
                      AND period = :period
                      AND gap_start = :gap_start
                      AND gap_end = :gap_end
                      AND status IN ('PENDING', 'RETRYING')
                    """
                ),
                {
                    "provider": PROVIDER_ID,
                    "dataset": item.dataset,
                    "period": period,
                    "gap_start": gap_start,
                    "gap_end": gap_end,
                    "resolved_at": checked_at,
                    "updated_at": checked_at,
                },
            )
            continue
        created_or_open += 1
        if _gap_exists(conn, item):
            conn.execute(
                text(
                    """
                    UPDATE sys_data_gap
                    SET last_run_id = :run_id,
                        last_error = :last_error,
                        next_retry_at = :next_retry_at,
                        updated_at = :updated_at
                    WHERE provider = :provider
                      AND dataset = :dataset
                      AND symbol = ''
                      AND period = :period
                      AND gap_start = :gap_start
                      AND gap_end = :gap_end
                      AND status IN ('PENDING', 'RETRYING')
                    """
                ),
                {
                    "run_id": run_id,
                    "last_error": f"coverage {item.actual_count}/{item.expected_count} ({item.coverage_ratio:.2%})",
                    "next_retry_at": checked_at + timedelta(hours=6),
                    "updated_at": checked_at,
                    "provider": PROVIDER_ID,
                    "dataset": item.dataset,
                    "period": period,
                    "gap_start": gap_start,
                    "gap_end": gap_end,
                },
            )
            continue
        conn.execute(
            text(
                """
                INSERT INTO sys_data_gap (
                    provider, dataset, symbol, period, gap_start, gap_end, reason,
                    status, retry_count, last_run_id, last_error, next_retry_at, updated_at
                ) VALUES (
                    :provider, :dataset, '', :period, :gap_start, :gap_end, :reason,
                    'PENDING', 0, :run_id, :last_error, :next_retry_at, :updated_at
                )
                """
            ),
            {
                "provider": PROVIDER_ID,
                "dataset": item.dataset,
                "period": period,
                "gap_start": gap_start,
                "gap_end": gap_end,
                "reason": "coverage_below_threshold_history_backfill_deferred",
                "run_id": run_id,
                "last_error": f"coverage {item.actual_count}/{item.expected_count} ({item.coverage_ratio:.2%})",
                "next_retry_at": checked_at + timedelta(hours=6),
                "updated_at": checked_at,
            },
        )
    return created_or_open


def _save_quality(conn, *, run_id: str, quality: Iterable[QualityResult], checked_at: datetime) -> int:
    count = 0
    for item in quality:
        count += 1
        conn.execute(
            text(
                """
                INSERT INTO sys_data_quality_result (
                    run_id, batch_id, provider, dataset, rule_name, status,
                    checked_rows, failed_rows, metric_value, threshold_value,
                    details_json, checked_at
                ) VALUES (
                    :run_id, :batch_id, :provider, :dataset, :rule_name, :status,
                    :checked_rows, :failed_rows, :metric_value, :threshold_value,
                    :details_json, :checked_at
                )
                """
            ),
            {
                "run_id": run_id,
                "batch_id": run_id,
                "provider": PROVIDER_ID,
                "dataset": item.dataset,
                "rule_name": item.rule_name,
                "status": item.status,
                "checked_rows": item.checked_rows,
                "failed_rows": item.failed_rows,
                "metric_value": item.metric_value,
                "threshold_value": item.threshold_value,
                "details_json": _json(item.details or {}),
                "checked_at": checked_at,
            },
        )
    return count


def run_nightly_reconciliation(engine: Engine, *, scan_days: int = 20) -> NightlyReconciliationResult:
    validate_audit_schema(engine)
    run_id = f"qmt_nightly_{_now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    started_at = _now()
    coverage: list[CoverageResult] = []
    quality: list[QualityResult] = []
    gaps_created_or_open = 0
    pending_replay_payload: dict[str, Any] = {}
    target_trade_date: date | None = None
    status = "SUCCESS"
    error_message: str | None = None

    try:
        dates = _previous_trade_dates(engine, scan_days=max(1, scan_days))
        target_trade_date = dates[0] if dates else None
        with engine.begin() as conn:
            _insert_sync_run_start(conn, run_id=run_id, target_trade_date=target_trade_date, started_at=started_at)

        pending_replay = replay_pending_writes(engine)
        pending_replay_payload = pending_result_dict(pending_replay)
        coverage = build_coverage_results(engine, scan_days=scan_days)
        quality = build_quality_results(engine, target_trade_date=target_trade_date)
        checked_at = _now()
        with engine.begin() as conn:
            _save_coverage(conn, run_id=run_id, coverage=coverage, checked_at=checked_at)
            gaps_created_or_open = _save_gaps(conn, run_id=run_id, coverage=coverage, checked_at=checked_at)
            _save_quality(conn, run_id=run_id, quality=quality, checked_at=checked_at)

        if any(item.status == "FAIL" for item in quality):
            status = "FAILED"
        elif any(item.status == "FAIL" for item in coverage) or gaps_created_or_open:
            status = "WARN"
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)

    finished_at = _now()
    expected_count = sum(item.expected_count for item in coverage)
    actual_count = sum(item.actual_count for item in coverage)
    missing_count = sum(item.missing_count for item in coverage)
    with engine.begin() as conn:
        _finish_sync_run(
            conn,
            run_id=run_id,
            status=status,
            finished_at=finished_at,
            expected_count=expected_count,
            actual_count=actual_count,
            missing_count=missing_count,
            error_message=error_message,
            extra={
                "pending_replay": pending_replay_payload,
                "coverage_rows": len(coverage),
                "quality_rows": len(quality),
                "gaps_created_or_open": gaps_created_or_open,
                "history_backfill": "deferred_queue",
            },
        )

    return NightlyReconciliationResult(
        run_id=run_id,
        status=status,
        target_trade_date=target_trade_date.isoformat() if target_trade_date else None,
        scan_days=scan_days,
        pending_replay=pending_replay_payload,
        coverage=[asdict(item) for item in coverage],
        quality=[asdict(item) for item in quality],
        gaps_created_or_open=gaps_created_or_open,
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        error_message=error_message,
    )


def result_dict(result: NightlyReconciliationResult) -> dict[str, Any]:
    return asdict(result)
