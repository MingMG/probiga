#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rebuild a Shanghai/Shenzhen daily capital-flow range from one provider.

Baidu's historical endpoint returns up to 20 sessions per request. This tool
paginates that endpoint once per stock, validates accounting identities and
per-date K-line-universe coverage, and is read-only unless ``--apply`` is
provided. The write replaces only the requested dates in one transaction.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, write_frame  # noqa: E402
from server.common.kline_data import get_kline_engine  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.fetch_sm_stock_capital_flow_daily import _convert_value  # noqa: E402


_ENDPOINT = "https://finance.pae.baidu.com/vapi/v1/fundsortlist"
_FIELDS = [
    "main_net_inflow", "max_net_inflow", "lg_net_inflow",
    "mid_net_inflow", "sm_net_inflow",
]
_ERROR_FIELDS = [
    f"_{field}_rounding_error"
    for field in _FIELDS
]
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://gushitong.baidu.com/",
}


@dataclass
class CodeOutcome:
    code: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


def _parse_content(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return [
        item
        for item in (value or [])
        if isinstance(item, dict)
    ] if isinstance(value, list) else []


def _parse_history_rows(
    content: Any,
    code: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in _parse_content(content):
        trade_date = str(item.get("date") or "").replace("/", "-")[:10]
        if len(trade_date) != 10:
            continue
        raw_fields = {
            "main_net_inflow": item.get("extMainIn"),
            "sm_net_inflow": item.get("littleNetIn"),
            "mid_net_inflow": item.get("mediumNetIn"),
            "lg_net_inflow": item.get("largeNetIn"),
            "max_net_inflow": item.get("superNetIn"),
        }
        parsed = {
            field: _provider_amount(value)
            for field, value in raw_fields.items()
        }
        if any(value is None for value in parsed.values()):
            continue
        record = {
            "stock_code": code,
            "trade_date": trade_date,
            "data_source": "baidu_history",
        }
        for field, value in parsed.items():
            assert value is not None
            record[field] = value[0]
            record[f"_{field}_rounding_error"] = value[1]
        if all(math.isfinite(float(record[field])) for field in _FIELDS):
            records.append(record)
    return records


def _provider_amount(
    raw_value: Any,
) -> tuple[float, float] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        value = float(raw_value)
        return (value, 0.0) if math.isfinite(value) else None
    text_value = raw_value.replace("元", "").strip()
    if not text_value or text_value == "--":
        return None
    matched = re.search(r"[-+]?\d*\.?\d+", text_value)
    if not matched:
        return None
    numeric_text = matched.group(0)
    value = float(_convert_value(raw_value))
    numeric_value = float(numeric_text)
    if not math.isfinite(value):
        return None
    if numeric_value != 0:
        multiplier = abs(value / numeric_value)
    elif "亿" in text_value:
        multiplier = 100_000_000.0
    elif "万" in text_value:
        multiplier = 10_000.0
    else:
        multiplier = 1.0
    decimal_places = (
        len(numeric_text.rsplit(".", 1)[1])
        if "." in numeric_text
        else 0
    )
    rounding_error = (
        0.5
        * (10.0 ** (-decimal_places))
        * multiplier
    )
    return value, rounding_error


def _fetch_page(code: str, before_date: str) -> list[dict[str, Any]]:
    response = requests.get(
        _ENDPOINT,
        params={
            "code": code,
            "market": "ab",
            "finance_type": "stock",
            "tab": "day",
            "from": "history",
            "date": before_date.replace("-", ""),
            "pn": 0,
            "rn": 100,
            "finClientType": "pc",
        },
        headers=_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("Result")
    if not isinstance(result, dict):
        return []
    return _parse_history_rows(result.get("content"), code)


def _fetch_code(
    code: str,
    target_dates: set[str],
    end_date: str,
) -> CodeOutcome:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            cursor = (
                datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            found: dict[str, dict[str, Any]] = {}
            for _page in range(6):
                page = _fetch_page(code, cursor)
                if not page:
                    cursor = (
                        datetime.strptime(cursor, "%Y-%m-%d")
                        + (
                            timedelta(days=1)
                            if found
                            else -timedelta(days=1)
                        )
                    ).strftime("%Y-%m-%d")
                    continue
                for row in page:
                    date_value = str(row["trade_date"])
                    if date_value in target_dates:
                        found[date_value] = row
                oldest = min(str(row["trade_date"]) for row in page)
                if target_dates.issubset(found) or oldest <= min(target_dates):
                    break
                cursor = oldest
            return CodeOutcome(
                code=code,
                rows=[
                    found[trade_date]
                    for trade_date in sorted(found)
                ],
            )
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            if attempt < 2:
                time.sleep(0.4 * (2 ** attempt) + random.uniform(0, 0.3))
    assert last_error is not None
    return CodeOutcome(
        code=code,
        error=f"{type(last_error).__name__}: {str(last_error)[:240]}",
    )


def _load_scope(
    start_date: str,
    end_date: str,
) -> tuple[list[str], dict[str, set[str]]]:
    engine = get_kline_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT stock_code, trade_date
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
              AND volume > 0
            ORDER BY stock_code, trade_date
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()
    by_date: dict[str, set[str]] = {}
    codes: set[str] = set()
    for code, trade_date in rows:
        normalized = str(code).strip().zfill(6)
        date_value = str(trade_date)[:10]
        codes.add(normalized)
        by_date.setdefault(date_value, set()).add(normalized)
    return sorted(codes), by_date


def _dataset_hash(frame: pd.DataFrame) -> str:
    canonical = frame[
        ["stock_code", "trade_date", *_FIELDS, "data_source"]
    ].sort_values(["trade_date", "stock_code"]).copy()
    canonical["stock_code"] = (
        canonical["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    canonical["trade_date"] = (
        canonical["trade_date"].astype(str).str[:10]
    )
    for field in _FIELDS:
        canonical[field] = (
            pd.to_numeric(canonical[field], errors="raise")
            .astype("float64")
            .round(2)
        )
    payload = canonical.to_json(orient="records", double_precision=2)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _identity_tolerances(
    frame: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    if set(_ERROR_FIELDS).issubset(frame.columns):
        main_tolerance = (
            frame["_main_net_inflow_rounding_error"]
            + frame["_lg_net_inflow_rounding_error"]
            + frame["_max_net_inflow_rounding_error"]
            + 1.0
        )
        balance_tolerance = (
            frame["_main_net_inflow_rounding_error"]
            + frame["_mid_net_inflow_rounding_error"]
            + frame["_sm_net_inflow_rounding_error"]
            + 1.0
        )
        return main_tolerance, balance_tolerance
    fallback = (
        frame[
            [
                "main_net_inflow", "max_net_inflow", "lg_net_inflow",
                "mid_net_inflow", "sm_net_inflow",
            ]
        ].abs().max(axis=1) * 0.001
    ).clip(lower=1_000_000.0)
    return fallback, fallback


def _identity_failure_counts(
    frame: pd.DataFrame,
) -> tuple[int, int]:
    main_component_delta = (
        frame["main_net_inflow"]
        - frame["lg_net_inflow"]
        - frame["max_net_inflow"]
    ).abs()
    market_balance_delta = (
        frame["main_net_inflow"]
        + frame["mid_net_inflow"]
        + frame["sm_net_inflow"]
    ).abs()
    main_tolerance, balance_tolerance = _identity_tolerances(frame)
    return (
        int(main_component_delta.gt(main_tolerance).sum()),
        int(market_balance_delta.gt(balance_tolerance).sum()),
    )


def _identity_failure_samples(
    frame: pd.DataFrame,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    main_component_delta = (
        frame["main_net_inflow"]
        - frame["lg_net_inflow"]
        - frame["max_net_inflow"]
    ).abs()
    market_balance_delta = (
        frame["main_net_inflow"]
        + frame["mid_net_inflow"]
        + frame["sm_net_inflow"]
    ).abs()
    main_tolerance, balance_tolerance = _identity_tolerances(frame)
    failing = frame[
        main_component_delta.gt(main_tolerance)
        | market_balance_delta.gt(balance_tolerance)
    ].copy()
    failing["main_component_delta"] = main_component_delta.loc[
        failing.index
    ]
    failing["market_balance_delta"] = market_balance_delta.loc[
        failing.index
    ]
    failing["main_component_tolerance"] = main_tolerance.loc[
        failing.index
    ]
    failing["market_balance_tolerance"] = balance_tolerance.loc[
        failing.index
    ]
    columns = [
        "stock_code", "trade_date", *_FIELDS,
        "main_component_delta", "market_balance_delta",
        "main_component_tolerance", "market_balance_tolerance",
    ]
    return failing.sort_values(
        ["trade_date", "stock_code"],
    )[columns].head(max(1, limit)).to_dict(orient="records")


def _write_staging(frame: pd.DataFrame, staging_path: str) -> Path:
    path = Path(staging_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "stock_code", "trade_date", *_FIELDS, "data_source",
        *[
            field
            for field in _ERROR_FIELDS
            if field in frame.columns
        ],
    ]
    frame[columns].sort_values(
        ["trade_date", "stock_code"],
    ).to_csv(
        path,
        index=False,
        encoding="utf-8",
        float_format="%.17g",
    )
    return path


def _read_staging(staging_path: str) -> pd.DataFrame:
    path = Path(staging_path).expanduser().resolve()
    frame = pd.read_csv(
        path,
        dtype={"stock_code": str, "trade_date": str},
    )
    required = {
        "stock_code", "trade_date", *_FIELDS, "data_source",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            f"capital-flow staging file missing columns: {missing}"
        )
    frame["stock_code"] = (
        frame["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    return frame


def rebuild(
    start_date: str,
    end_date: str,
    *,
    workers: int = 16,
    min_coverage: float = 0.99,
    apply: bool = False,
    staging_path: str = "",
    from_staging: str = "",
    fill_staging_gaps: bool = False,
) -> int:
    codes, expected_codes_by_date = _load_scope(start_date, end_date)
    target_dates = sorted(expected_codes_by_date)
    if not codes or not target_dates:
        print("No Shanghai/Shenzhen traded K-line universe in requested range")
        return 2
    print(
        f"Baidu capital-flow rebuild: {start_date}..{end_date}, "
        f"dates={len(target_dates)}, codes={len(codes)}, "
        f"workers={workers}, apply={apply}"
    )
    errors: list[CodeOutcome] = []
    if from_staging:
        frame = _read_staging(from_staging)
        if fill_staging_gaps:
            existing_keys = set(zip(
                frame["stock_code"],
                frame["trade_date"],
            ))
            missing_codes = sorted({
                code
                for trade_date, expected_codes in expected_codes_by_date.items()
                for code in expected_codes
                if (code, trade_date) not in existing_keys
            })
            print(
                f"Filling staging gaps from Baidu: "
                f"codes={len(missing_codes)}",
                flush=True,
            )
            gap_outcomes: list[CodeOutcome] = []
            with ThreadPoolExecutor(
                max_workers=max(1, min(workers, 24)),
            ) as pool:
                futures = [
                    pool.submit(
                        _fetch_code,
                        code,
                        set(target_dates),
                        end_date,
                    )
                    for code in missing_codes
                ]
                for future in as_completed(futures):
                    gap_outcomes.append(future.result())
            errors = [
                outcome
                for outcome in gap_outcomes
                if outcome.error
            ]
            gap_records = [
                row
                for outcome in gap_outcomes
                for row in outcome.rows
            ]
            if gap_records:
                frame = pd.concat(
                    [frame, pd.DataFrame(gap_records)],
                    ignore_index=True,
                ).drop_duplicates(
                    ["stock_code", "trade_date"],
                    keep="last",
                )
        frame = frame[
            frame.apply(
                lambda row: (
                    row["stock_code"]
                    in expected_codes_by_date.get(
                        str(row["trade_date"]),
                        set(),
                    )
                ),
                axis=1,
            )
        ].copy()
        print(f"Loaded staging artifact: {Path(from_staging).resolve()}")
    else:
        outcomes: list[CodeOutcome] = []
        with ThreadPoolExecutor(
            max_workers=max(1, min(workers, 24)),
        ) as pool:
            futures = {
                pool.submit(
                    _fetch_code,
                    code,
                    set(target_dates),
                    end_date,
                ): code
                for code in codes
            }
            for done, future in enumerate(
                as_completed(futures),
                start=1,
            ):
                outcome = future.result()
                outcomes.append(outcome)
                if outcome.error:
                    print(
                        f"  {outcome.code} error: {outcome.error}",
                        flush=True,
                    )
                if done % 250 == 0 or done == len(futures):
                    print(
                        f"  progress {done}/{len(futures)}",
                        flush=True,
                    )

        errors = [
            outcome
            for outcome in outcomes
            if outcome.error
        ]
        records = [
            row
            for outcome in outcomes
            for row in outcome.rows
            if row["stock_code"] in expected_codes_by_date.get(
                str(row["trade_date"]),
                set(),
            )
        ]
        frame = pd.DataFrame(records)
    if frame.empty:
        print("No capital-flow rows fetched")
        return 3
    frame = frame.drop_duplicates(
        ["stock_code", "trade_date"],
        keep="last",
    )
    for field in [
        *_FIELDS,
        *[
            column
            for column in _ERROR_FIELDS
            if column in frame.columns
        ],
    ]:
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    staged_artifact = ""
    if staging_path:
        staged_artifact = str(_write_staging(frame, staging_path))
        print(f"Staged fetched rows: {staged_artifact}", flush=True)
    (
        main_component_failures,
        market_balance_failures,
    ) = _identity_failure_counts(
        frame,
    )
    identity_failures = (
        main_component_failures + market_balance_failures
    )
    coverage_by_date = {
        trade_date: (
            int((frame["trade_date"] == trade_date).sum())
            / max(len(expected_codes_by_date[trade_date]), 1)
        )
        for trade_date in target_dates
    }
    summary = {
        "status": "pass",
        "target_dates": target_dates,
        "codes": len(codes),
        "expected_by_date": {
            key: len(value)
            for key, value in expected_codes_by_date.items()
        },
        "fetched_by_date": {
            key: int((frame["trade_date"] == key).sum())
            for key in target_dates
        },
        "coverage_by_date": {
            key: round(value, 6)
            for key, value in coverage_by_date.items()
        },
        "worker_errors": len(errors),
        "identity_failures": identity_failures,
        "main_component_identity_failures": main_component_failures,
        "market_balance_identity_failures": market_balance_failures,
        "identity_failure_samples": _identity_failure_samples(frame),
        "error_samples": [
            {"stock_code": item.code, "error": item.error}
            for item in errors[:20]
        ],
        "dataset_sha256": _dataset_hash(frame),
        "staging_artifact": staged_artifact or from_staging,
    }
    blocked = (
        bool(errors)
        or identity_failures > 0
        or any(
            coverage < min_coverage
            for coverage in coverage_by_date.values()
        )
    )
    if blocked:
        summary["status"] = "fail"
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print("Capital-flow rebuild blocked; existing rows retained")
        return 3
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not apply:
        print("[dry-run] all gates passed; database unchanged")
        return 0

    frame["etl_sync_at"] = datetime.now().replace(microsecond=0)
    engine = create_batch_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM sm_stock_capital_flow_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
        """), {
            "start_date": start_date,
            "end_date": end_date,
        })
        write_frame(
            frame[
                [
                    "stock_code", "trade_date", *_FIELDS,
                    "data_source", "etl_sync_at",
                ]
            ],
            "sm_stock_capital_flow_daily",
            conn,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    print(f"Capital-flow rebuild completed: rows={len(frame)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--min-coverage", type=float, default=0.99)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--staging-path",
        default="",
        help="Write fetched rows to a reusable CSV artifact before gates",
    )
    parser.add_argument(
        "--from-staging",
        default="",
        help="Validate/apply a previously staged CSV without refetching",
    )
    parser.add_argument(
        "--fill-staging-gaps",
        action="store_true",
        help="With --from-staging, refetch only missing keys from Baidu",
    )
    args = parser.parse_args()
    engine = create_batch_engine()
    with mysql_named_lock(
        engine,
        "probiga:capital_flow_daily",
        timeout_seconds=int(
            os.environ.get("FLOW_DAILY_LOCK_TIMEOUT", "60")
        ),
    ):
        return rebuild(
            args.start_date,
            args.end_date,
            workers=max(1, args.workers),
            min_coverage=max(0.0, min(1.0, args.min_coverage)),
            apply=args.apply,
            staging_path=args.staging_path,
            from_staging=args.from_staging,
            fill_staging_gaps=args.fill_staging_gaps,
        )


if __name__ == "__main__":
    raise SystemExit(main())
