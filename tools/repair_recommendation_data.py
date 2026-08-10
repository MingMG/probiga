"""Repair the data prerequisites for the AI recommendation batch.

The recommendation page must not silently turn a partial data day into an
empty recommendation list. This module checks the target trading day first,
repairs the missing daily K-line / historical capital-flow / snapshot data,
and returns a machine-readable coverage report for the worker and UI.

It deliberately does not place orders or touch any broker account.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.common.process_env import build_child_env, child_process_timeout


ProgressCallback = Callable[[str, dict[str, Any]], None]


def _scalar(engine, sql: str, params: dict[str, Any] | None = None) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _code_set(engine, sql: str, params: dict[str, Any] | None = None) -> set[str]:
    with engine.connect() as conn:
        return {
            str(row[0]).strip().zfill(6)
            for row in conn.execute(text(sql), params or {}).fetchall()
            if str(row[0] or "").strip()
        }


def _recommendation_universe(engine, trade_date: str) -> tuple[set[str], int, int, str, Any]:
    """Resolve the exact traded universe from the same K-line DB used by analysis."""
    try:
        kline_engine = get_kline_engine()
    except Exception:
        kline_engine = engine

    try:
        all_code_set = _code_set(
            engine,
            """
            SELECT stock_code
            FROM si_all_code
            WHERE stock_code REGEXP '^(0|3|6)'
            """,
        )
    except Exception:
        all_code_set = set()

    try:
        target_kline_codes = _code_set(
            kline_engine,
            """
            SELECT DISTINCT stock_code
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
            """,
            {"d": trade_date},
        )
    except Exception:
        target_kline_codes = set()

    all_code_expected = len(all_code_set)
    target_kline_expected = len(target_kline_codes)
    if target_kline_expected and (
        not all_code_expected or target_kline_expected >= int(all_code_expected * 0.95)
    ):
        universe = set(target_kline_codes)
        if all_code_set:
            universe &= all_code_set
        return (
            universe,
            len(universe) or target_kline_expected,
            all_code_expected,
            "target_day_traded_kline_intersect_catalog",
            kline_engine,
        )
    if all_code_set:
        return all_code_set, all_code_expected, all_code_expected, "si_all_code", kline_engine

    try:
        previous_codes = _code_set(
            kline_engine,
            """
            SELECT DISTINCT stock_code
            FROM sm_stock_kline
            WHERE trade_date = (
                SELECT MAX(trade_date) FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0 AND trade_date < :d
            ) AND k_type = 1 AND adjust_type = 0
            """,
            {"d": trade_date},
        )
    except Exception:
        previous_codes = set()
    return previous_codes, len(previous_codes), 0, "previous_kline_trade_date", kline_engine


def _missing_source_codes(engine, trade_date: str, source: str) -> list[str]:
    table_by_source = {
        "capital_flow": "sm_stock_capital_flow_daily",
        "snapshot": "sm_stock_snapshot",
    }
    table = table_by_source.get(source)
    if not table:
        raise ValueError(f"unsupported source: {source}")
    universe, _expected, _all_expected, _source, _kline_engine = _recommendation_universe(
        engine,
        trade_date,
    )
    if not universe:
        return []
    available = _code_set(
        engine,
        f"SELECT DISTINCT stock_code FROM {table} WHERE trade_date = :d",
        {"d": trade_date},
    )
    return sorted(universe - available)


def coverage_report(engine, trade_date: str) -> dict[str, Any]:
    """Return source coverage for a target trading day without writing data."""
    (
        recommendation_universe,
        expected,
        all_code_expected,
        universe_source,
        kline_engine,
    ) = _recommendation_universe(engine, trade_date)

    queries = {
        "kline": """
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
        """,
        "snapshot": """
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_snapshot
            WHERE trade_date = :d
        """,
        "capital_flow": """
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_capital_flow_daily
            WHERE trade_date = :d
        """,
        "hot_rank": """
            SELECT COUNT(DISTINCT stock_code)
            FROM st_hot_rank_fused
            WHERE snapshot_date = :d
        """,
        "analysis": """
            SELECT COUNT(DISTINCT stock_code)
            FROM stock_analysis_result
            WHERE analysis_date = :d
        """,
        "recommendation": """
            SELECT COUNT(*)
            FROM st_recommended_stocks
            WHERE pick_date = :d
        """,
    }
    counts: dict[str, int] = {}
    for key, sql in queries.items():
        try:
            read_engine = kline_engine if key == "kline" else engine
            counts[key] = int(_scalar(read_engine, sql, {"d": trade_date}) or 0)
        except Exception:
            counts[key] = 0

    missing_by_source: dict[str, dict[str, Any]] = {}
    if recommendation_universe:
        try:
            with engine.connect() as conn:
                flow_codes = {
                    str(row[0]).zfill(6)
                    for row in conn.execute(text("""
                        SELECT DISTINCT stock_code
                        FROM sm_stock_capital_flow_daily
                        WHERE trade_date = :d
                    """), {"d": trade_date}).fetchall()
                }
                snapshot_codes = {
                    str(row[0]).zfill(6)
                    for row in conn.execute(text("""
                        SELECT DISTINCT stock_code
                        FROM sm_stock_snapshot
                        WHERE trade_date = :d
                    """), {"d": trade_date}).fetchall()
                }
            for key, code_set in (("capital_flow", flow_codes), ("snapshot", snapshot_codes)):
                missing = sorted(recommendation_universe - code_set)
                missing_by_source[key] = {
                    "count": len(missing),
                    "sample": missing[:20],
                }
        except Exception:
            missing_by_source = {}

    def item(key: str, minimum: float, required: bool = True) -> dict[str, Any]:
        count = counts[key]
        ratio = round(min(count / expected, 1.0), 4) if expected else 0.0
        missing = missing_by_source.get(key)
        exact_ready = missing is None or int(missing.get("count") or 0) == 0
        return {
            "key": key,
            "count": count,
            "expected": expected,
            "coverage": ratio,
            "minimum": minimum,
            "required": required,
            "missing_count": int(missing.get("count") or 0) if missing else 0,
            "missing_sample": list(missing.get("sample") or []) if missing else [],
            "ready": bool(expected and ratio >= minimum and exact_ready),
        }

    return {
        "trade_date": trade_date,
        "expected_stocks": expected,
        "expected_all_stocks": all_code_expected,
        "universe_source": universe_source,
        "sources": [
            item("kline", 0.80),
            item("capital_flow", 0.70),
            item("snapshot", 0.70),
            item("hot_rank", 0.01, required=False),
            item("analysis", 0.80, required=False),
            item("recommendation", 0.01, required=False),
        ],
        "counts": counts,
    }


def _run_command(
    command: list[str],
    *,
    callback: ProgressCallback | None,
    stage: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    if callback:
        callback(stage, {"command": command})
    env = build_child_env(ROOT)
    timeout = child_process_timeout(timeout_seconds, env_name="PROBIGA_RECOMMENDATION_REPAIR_TIMEOUT")
    started = datetime.now().isoformat(timespec="seconds")
    try:
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            env=env,
            text=True,
            timeout=timeout,
            capture_output=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{stage} timeout after {timeout}s") from exc
    output = (completed.stdout or "") + (completed.stderr or "")
    result = {
        "stage": stage,
        "started_at": started,
        "returncode": int(completed.returncode),
        "output_tail": output[-1200:],
    }
    if completed.returncode != 0:
        raise RuntimeError(f"{stage} failed (exit={completed.returncode}): {output[-800:]}")
    return result


def repair_target_data(
    trade_date: str,
    *,
    min_kline_coverage: float = 0.80,
    min_flow_coverage: float = 0.70,
    min_snapshot_coverage: float = 0.70,
    timeout_seconds: int = 6 * 60 * 60,
    callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Repair missing prerequisites and return the final coverage report."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required")
    engine = create_batch_engine()
    before = coverage_report(engine, trade_date)
    actions: list[dict[str, Any]] = []
    expected = int(before.get("expected_stocks") or 0)

    def ready(report: dict[str, Any], key: str, minimum: float) -> bool:
        source = next((x for x in report["sources"] if x["key"] == key), {})
        return bool(
            source.get("ready")
            and float(source.get("coverage") or 0.0) >= minimum
        )

    if not ready(before, "kline", min_kline_coverage):
        actions.append(
            _run_command(
                [
                    sys.executable,
                    str(ROOT / "tools" / "fetch_sm_stock_kline_daily.py"),
                    trade_date,
                    "--min-coverage",
                    str(min_kline_coverage),
                ],
                callback=callback,
                stage="repair_daily_kline",
                timeout_seconds=timeout_seconds,
            )
        )

    # Retry only the still-missing symbols. A single gap must never trigger a
    # destructive or overlapping full-market refresh again.
    for attempt in range(1, 3):
        current = coverage_report(engine, trade_date)
        if ready(current, "capital_flow", min_flow_coverage):
            break
        missing_codes = _missing_source_codes(engine, trade_date, "capital_flow")
        command = [
            sys.executable,
            str(ROOT / "tools" / "sync_capital_flow_direct.py"),
            "--date",
            trade_date,
            "--skip-truncate",
            "--sleep",
            "0.05",
        ]
        if missing_codes:
            command.extend(["--codes", ",".join(missing_codes)])
        actions.append(
            _run_command(
                command,
                callback=callback,
                stage=(
                    "repair_historical_capital_flow"
                    if attempt == 1
                    else "retry_historical_capital_flow_gaps"
                ),
                timeout_seconds=timeout_seconds,
            )
        )

    current = coverage_report(engine, trade_date)
    if not ready(current, "snapshot", min_snapshot_coverage):
        actions.append(
            _run_command(
                [
                    sys.executable,
                    str(ROOT / "biz" / "stock_market" / "sync_stock_snapshot.py"),
                    "--date",
                    trade_date,
                ],
                callback=callback,
                stage="repair_stock_snapshot",
                timeout_seconds=timeout_seconds,
            )
        )

    after = coverage_report(engine, trade_date)
    ready_for_recommendation = bool(
        expected
        and ready(after, "kline", min_kline_coverage)
        and ready(after, "capital_flow", min_flow_coverage)
        and ready(after, "snapshot", min_snapshot_coverage)
    )
    report = {
        "status": "ready" if ready_for_recommendation else "blocked",
        "trade_date": trade_date,
        "expected_stocks": expected,
        "ready_for_recommendation": ready_for_recommendation,
        "before": before,
        "after": after,
        "actions": actions,
        "missing_required": [
            item["key"]
            for item in after["sources"]
            if item.get("required") and not item.get("ready")
        ],
    }
    if not ready_for_recommendation:
        raise RuntimeError("recommendation data remains incomplete: " + json.dumps(report, ensure_ascii=False))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair recommendation data prerequisites for one A-share trading day")
    parser.add_argument("--date", required=True, help="Target trading date YYYY-MM-DD")
    parser.add_argument("--min-kline-coverage", type=float, default=0.80)
    parser.add_argument("--min-flow-coverage", type=float, default=0.70)
    parser.add_argument("--min-snapshot-coverage", type=float, default=0.70)
    parser.add_argument("--timeout-seconds", type=int, default=6 * 60 * 60)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = repair_target_data(
        args.date,
        min_kline_coverage=max(0.0, min(1.0, args.min_kline_coverage)),
        min_flow_coverage=max(0.0, min(1.0, args.min_flow_coverage)),
        min_snapshot_coverage=max(0.0, min(1.0, args.min_snapshot_coverage)),
        timeout_seconds=max(60, args.timeout_seconds),
        callback=lambda stage, _payload: print(f"REPAIR {stage}", flush=True),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
