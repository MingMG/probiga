#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit capital-flow and dragon-tiger inputs used by the unified screener.

The command is read-only. Capital-flow coverage is measured against the
Shanghai/Shenzhen stocks that actually traded on each date. Dragon-tiger rows
are checked for date coverage, duplicate stock/date keys, valid A-share codes,
and listing-date consistency. Codes absent from the current master remain
visible as informational because valid delisted history must not fail an audit.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.kline_data import get_kline_engine  # noqa: E402


DIRECT_QMT_FLOW_SOURCE = "gj_big_qmt_inner"


def _flow_identity_failures(
    source: str,
    values: list[float],
) -> tuple[bool, bool]:
    main, maximum, large, middle, small = values
    tolerance = max(max(abs(value) for value in values) * 0.001, 1_000_000.0)
    main_failure = abs(main - maximum - large) > tolerance
    balance_failure = (
        source.strip().lower() != DIRECT_QMT_FLOW_SOURCE
        and abs(main + middle + small) > tolerance
    )
    return main_failure, balance_failure


def _date(value: Any) -> str:
    return str(value or "")[:10]


def _identity(value: Any) -> str:
    return "<NULL>" if value is None else str(value)


def audit_inputs(
    start_date: str,
    end_date: str,
    *,
    min_flow_coverage: float = 0.99,
) -> dict[str, Any]:
    business_engine = create_batch_engine()
    kline_engine = get_kline_engine()
    with business_engine.connect() as conn:
        trade_dates = [
            _date(row[0])
            for row in conn.execute(text("""
                SELECT trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date BETWEEN :start_date AND :end_date
                ORDER BY trade_date
            """), {
                "start_date": start_date,
                "end_date": end_date,
            }).fetchall()
        ]
        flow_rows = conn.execute(text("""
            SELECT
              stock_code, trade_date, main_net_inflow, max_net_inflow,
              lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
            FROM sm_stock_capital_flow_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
            ORDER BY trade_date, stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).mappings().all()
        lhb_rows = conn.execute(text("""
            SELECT stock_code, trade_date
            FROM st_a_list_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date, stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).mappings().all()
        lhb_info_rows = conn.execute(text("""
            SELECT
              stock_code, trade_date, operate_code, operate_name,
              a_net_amount, a_buy_amount, a_sell_amount,
              a_buy_amount_rate, a_sell_amount_rate, reason
            FROM st_a_list_info
            WHERE trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date, stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).mappings().all()
        master_rows = conn.execute(text("""
            SELECT stock_code, list_date
            FROM si_all_code
            WHERE stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
        """)).fetchall()

    lhb_candidate_codes = sorted({
        str(row["stock_code"] or "").strip().zfill(6)
        for row in lhb_rows
        if row["stock_code"] is not None
    })
    with kline_engine.connect() as conn:
        traded_rows = conn.execute(text("""
            SELECT DISTINCT stock_code, trade_date
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
              AND volume > 0
            ORDER BY trade_date, stock_code
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
        historical_master_rows = []
        if lhb_candidate_codes:
            historical_master_rows = conn.execute(
                text("""
                    SELECT stock_code, MIN(trade_date) AS first_trade_date
                    FROM sm_stock_kline
                    WHERE stock_code IN :codes
                      AND trade_date <= :end_date
                      AND k_type = 1 AND adjust_type = 0
                    GROUP BY stock_code
                """).bindparams(bindparam("codes", expanding=True)),
                {"codes": lhb_candidate_codes, "end_date": end_date},
            ).fetchall()

    expected_by_date: dict[str, set[str]] = {
        trade_date: set()
        for trade_date in trade_dates
    }
    for code, trade_date in traded_rows:
        expected_by_date.setdefault(_date(trade_date), set()).add(
            str(code).strip().zfill(6)
        )

    flow_by_date: dict[str, set[str]] = {
        trade_date: set()
        for trade_date in trade_dates
    }
    flow_keys: list[tuple[str, str]] = []
    source_counts: dict[str, int] = {}
    missing_data_source_rows = 0
    non_finite_rows = 0
    main_component_identity_failures = 0
    market_balance_identity_failures = 0
    for row in flow_rows:
        code = str(row["stock_code"]).strip().zfill(6)
        trade_date = _date(row["trade_date"])
        flow_by_date.setdefault(trade_date, set()).add(code)
        flow_keys.append((code, trade_date))
        source = str(row["data_source"] or "")
        source_counts[source] = source_counts.get(source, 0) + 1
        if not source.strip():
            missing_data_source_rows += 1
        values = [
            float(row[column])
            for column in (
                "main_net_inflow", "max_net_inflow", "lg_net_inflow",
                "mid_net_inflow", "sm_net_inflow",
            )
        ]
        if not all(math.isfinite(value) for value in values):
            non_finite_rows += 1
            continue
        main_failure, balance_failure = _flow_identity_failures(source, values)
        if main_failure:
            main_component_identity_failures += 1
        if balance_failure:
            market_balance_identity_failures += 1

    flow_coverage = {
        trade_date: (
            len(flow_by_date.get(trade_date, set())
                & expected_by_date.get(trade_date, set()))
            / max(len(expected_by_date.get(trade_date, set())), 1)
        )
        for trade_date in trade_dates
    }
    flow_missing_samples = {
        trade_date: sorted(
            expected_by_date.get(trade_date, set())
            - flow_by_date.get(trade_date, set())
        )[:20]
        for trade_date in trade_dates
        if (
            expected_by_date.get(trade_date, set())
            - flow_by_date.get(trade_date, set())
        )
    }

    master = {
        str(code).strip().zfill(6): _date(list_date)
        for code, list_date in master_rows
    }
    for code, first_trade_date in historical_master_rows:
        master.setdefault(str(code).strip().zfill(6), _date(first_trade_date))
    lhb_keys: list[tuple[str, str]] = []
    out_of_scope_lhb_codes: set[str] = set()
    out_of_scope_lhb_rows = 0
    unknown_lhb_codes: set[str] = set()
    prelisting_lhb_rows = 0
    lhb_by_date = {
        trade_date: 0
        for trade_date in trade_dates
    }
    for row in lhb_rows:
        code = str(row["stock_code"] or "").strip().zfill(6)
        trade_date = _date(row["trade_date"])
        if (
            len(code) != 6
            or not code.isdigit()
            or code[:2] not in {"00", "30", "60", "68", "92"}
        ):
            out_of_scope_lhb_codes.add(code)
            out_of_scope_lhb_rows += 1
            continue
        lhb_keys.append((code, trade_date))
        lhb_by_date[trade_date] = lhb_by_date.get(trade_date, 0) + 1
        if code not in master:
            unknown_lhb_codes.add(code)
        elif master[code] and master[code] > trade_date:
            prelisting_lhb_rows += 1

    flow_duplicate_count = len(flow_keys) - len(set(flow_keys))
    lhb_duplicate_count = len(lhb_keys) - len(set(lhb_keys))
    lhb_key_set = set(lhb_keys)
    lhb_info_keys: list[tuple[str, str]] = []
    lhb_info_row_keys: list[tuple[Any, ...]] = []
    for row in lhb_info_rows:
        code = str(row["stock_code"] or "").strip().zfill(6)
        trade_date = _date(row["trade_date"])
        if (
            len(code) != 6
            or not code.isdigit()
            or code[:2] not in {"00", "30", "60", "68", "92"}
        ):
            continue
        lhb_info_keys.append((code, trade_date))
        lhb_info_row_keys.append((
            code,
            trade_date,
            _identity(row["operate_code"]),
            _identity(row["operate_name"]),
            _identity(row["a_net_amount"]),
            _identity(row["a_buy_amount"]),
            _identity(row["a_sell_amount"]),
            _identity(row["a_buy_amount_rate"]),
            _identity(row["a_sell_amount_rate"]),
            _identity(row["reason"]),
        ))
    lhb_info_key_set = set(lhb_info_keys)
    missing_lhb_info_keys = sorted(lhb_key_set - lhb_info_key_set)
    orphan_lhb_info_keys = sorted(lhb_info_key_set - lhb_key_set)
    duplicate_lhb_info_rows = (
        len(lhb_info_row_keys) - len(set(lhb_info_row_keys))
    )
    missing_lhb_dates = [
        trade_date
        for trade_date in trade_dates
        if lhb_by_date.get(trade_date, 0) == 0
    ]
    hard_failures = {
        "missing_trade_dates_for_flow": [
            trade_date
            for trade_date in trade_dates
            if not flow_by_date.get(trade_date)
        ],
        "flow_dates_below_minimum_coverage": [
            trade_date
            for trade_date, coverage in flow_coverage.items()
            if coverage < min_flow_coverage
        ],
        "flow_duplicate_business_keys": flow_duplicate_count,
        "flow_non_finite_rows": non_finite_rows,
        "flow_main_component_identity_failures": (
            main_component_identity_failures
        ),
        "flow_market_balance_identity_failures": (
            market_balance_identity_failures
        ),
        "flow_missing_data_source_rows": missing_data_source_rows,
        "missing_lhb_trade_dates": missing_lhb_dates,
        "lhb_duplicate_business_keys": lhb_duplicate_count,
        "prelisting_lhb_rows": prelisting_lhb_rows,
        "missing_lhb_info_keys": [
            {"stock_code": code, "trade_date": trade_date}
            for code, trade_date in missing_lhb_info_keys[:200]
        ],
        "missing_lhb_info_key_count": len(missing_lhb_info_keys),
        "orphan_lhb_info_keys": [
            {"stock_code": code, "trade_date": trade_date}
            for code, trade_date in orphan_lhb_info_keys[:200]
        ],
        "orphan_lhb_info_key_count": len(orphan_lhb_info_keys),
        "lhb_info_duplicate_rows": duplicate_lhb_info_rows,
    }
    informational = {
        "flow_source_count": len(source_counts),
        "lhb_codes_absent_from_current_master_and_kline": sorted(
            unknown_lhb_codes
        ),
    }
    failed = any(
        bool(value)
        for value in hard_failures.values()
    )
    return {
        "status": "fail" if failed else "pass",
        "start_date": start_date,
        "end_date": end_date,
        "trade_date_count": len(trade_dates),
        "capital_flow": {
            "row_count": len(flow_rows),
            "source_counts": source_counts,
            "expected_by_date": {
                key: len(value)
                for key, value in expected_by_date.items()
            },
            "actual_by_date": {
                key: len(value)
                for key, value in flow_by_date.items()
            },
            "coverage_by_date": {
                key: round(value, 6)
                for key, value in flow_coverage.items()
            },
            "missing_code_samples": flow_missing_samples,
        },
        "dragon_tiger": {
            "source_row_count": len(lhb_rows),
            "a_share_row_count": len(lhb_keys),
            "rows_by_date": lhb_by_date,
            "out_of_scope_row_count": out_of_scope_lhb_rows,
            "out_of_scope_codes": sorted(out_of_scope_lhb_codes),
            "info_source_row_count": len(lhb_info_rows),
            "info_a_share_key_count": len(lhb_info_keys),
            "info_distinct_stock_date_count": len(lhb_info_key_set),
        },
        "hard_failures": hard_failures,
        "informational": informational,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-flow-coverage", type=float, default=0.99)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    report = audit_inputs(
        args.start_date,
        args.end_date,
        min_flow_coverage=max(
            0.0,
            min(1.0, args.min_flow_coverage),
        ),
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
        print(path)
    else:
        print(payload)
    return 0 if report["status"] == "pass" else 3


if __name__ == "__main__":
    raise SystemExit(main())
