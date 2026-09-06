# -*- coding: utf-8 -*-
"""Read-only verification receipt for the direct QMT daily-flow partition."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (  # noqa: E402
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.batch_db import (  # noqa: E402
    create_batch_engine,
    routed_read_engine,
)


RECEIPT_SCHEMA = "probiga.direct-capital-flow-daily-verification.v1"
TASK_TYPE = "capital_flow_batch_fast"
DATASET = "stock_capital_flow_daily"
PROVIDER = "gj_big_qmt_inner"
CLOSE_READY_TIME = time(15, 40)
FLOW_FIELDS = (
    "main_net_inflow",
    "max_net_inflow",
    "lg_net_inflow",
    "mid_net_inflow",
    "sm_net_inflow",
)
FLOW_BALANCE_TOLERANCE = Decimal("0.01")
_CODE_RE = re.compile(r"^[0-9]{6}$")
_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")


def _canonical_date(value: object) -> str:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise RuntimeError("DATA_BLOCKED: capital-flow trade date is invalid") from exc
    if raw != parsed:
        raise RuntimeError("DATA_BLOCKED: capital-flow trade date is invalid")
    return parsed


def _read_rows(
    engine: object,
    sql: str,
    params: Mapping[str, Any],
) -> list[dict[str, Any]]:
    read_engine = routed_read_engine(sql, engine)
    with read_engine.connect() as connection:
        result = connection.execute(text(sql), dict(params))
        return [dict(row) for row in result.mappings().all()]


def _stock_code(value: object) -> str:
    code = str(value or "").strip()
    if _CODE_RE.fullmatch(code) is None:
        raise RuntimeError("DATA_BLOCKED: capital-flow partition has invalid stock code")
    return code


def _decimal_text(value: object, *, field: str, stock_code: str) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow value is not numeric: "
            f"stock_code={stock_code} field={field}"
        ) from exc
    if not number.is_finite():
        raise RuntimeError(
            "DATA_BLOCKED: capital-flow value is not finite: "
            f"stock_code={stock_code} field={field}"
        )
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def _sha256_json(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def inspect_partition(engine: object, trade_date: str) -> dict[str, Any]:
    """Verify and identify one exact formal QMT partition without writing."""

    target = _canonical_date(trade_date)
    expected_rows = _read_rows(
        engine,
        """
        SELECT stock_code
          FROM sm_stock_kline
         WHERE trade_date=:trade_date
           AND k_type=1
           AND adjust_type=0
           AND volume>0
           AND amount>0
           AND SUBSTR(stock_code, 1, 2) IN ('00','30','60','68')
         ORDER BY stock_code
        """,
        {"trade_date": target},
    )
    expected_codes = [_stock_code(row.get("stock_code")) for row in expected_rows]
    if not expected_codes:
        raise RuntimeError(
            "DATA_BLOCKED: target-date traded SH/SZ K-line universe is empty"
        )
    if len(expected_codes) != len(set(expected_codes)):
        raise RuntimeError(
            "DATA_BLOCKED: target-date traded SH/SZ K-line universe is duplicated"
        )

    flow_rows = _read_rows(
        engine,
        """
        SELECT stock_code, trade_date, main_net_inflow, max_net_inflow,
               lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
          FROM sm_stock_capital_flow_daily
         WHERE trade_date=:trade_date
         ORDER BY stock_code
        """,
        {"trade_date": target},
    )
    if not flow_rows:
        raise RuntimeError("DATA_BLOCKED: direct QMT capital-flow partition is empty")

    canonical_rows: list[dict[str, str]] = []
    actual_codes: list[str] = []
    for row in flow_rows:
        code = _stock_code(row.get("stock_code"))
        actual_codes.append(code)
        row_date = str(row.get("trade_date") or "")[:10]
        if row_date != target:
            raise RuntimeError(
                "DATA_BLOCKED: direct QMT capital-flow row has another trade date"
            )
        source = str(row.get("data_source") or "").strip().lower()
        if source != PROVIDER:
            raise RuntimeError(
                "DATA_BLOCKED: direct QMT capital-flow partition has another source: "
                f"stock_code={code} source={source or 'EMPTY'}"
            )
        values = {
            field: _decimal_text(row.get(field), field=field, stock_code=code)
            for field in FLOW_FIELDS
        }
        if abs(
            Decimal(values["main_net_inflow"])
            - Decimal(values["max_net_inflow"])
            - Decimal(values["lg_net_inflow"])
        ) > FLOW_BALANCE_TOLERANCE:
            raise RuntimeError(
                "DATA_BLOCKED: direct QMT capital-flow identity differs: "
                f"stock_code={code} main_net_inflow must equal "
                "max_net_inflow + lg_net_inflow"
            )
        canonical_rows.append(
            {
                "stock_code": code,
                "trade_date": target,
                **values,
                "data_source": source,
            }
        )

    if len(actual_codes) != len(set(actual_codes)):
        raise RuntimeError(
            "DATA_BLOCKED: direct QMT capital-flow partition has duplicate codes"
        )
    expected_set = set(expected_codes)
    actual_set = set(actual_codes)
    if actual_set != expected_set:
        raise RuntimeError(
            "DATA_BLOCKED: direct QMT capital-flow coverage differs from traded "
            f"SH/SZ universe: missing={len(expected_set - actual_set)} "
            f"unexpected={len(actual_set - expected_set)}"
        )

    canonical_rows.sort(key=lambda item: item["stock_code"])
    sorted_codes = sorted(actual_set)
    return {
        "trade_date": target,
        "row_count": len(canonical_rows),
        "expected_row_count": len(expected_set),
        "code_set_sha256": _sha256_json(sorted_codes),
        "partition_sha256": _sha256_json(canonical_rows),
        "source_counts": {PROVIDER: len(canonical_rows)},
    }


def _scheduler_build_sha() -> str:
    value = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if _SHA40_RE.fullmatch(value) is None or value == "0" * 40:
        raise RuntimeError(
            "DATA_BLOCKED: direct QMT verifier build SHA is unavailable"
        )
    return value


def resolve_trade_date(
    engine: object,
    requested: str | None,
    *,
    now: datetime | None = None,
) -> str:
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=PRODUCTION_TIMEZONE)
    else:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    latest_closed = authoritative_closed_trade_date(
        engine,
        now=current,
        close_ready_time=CLOSE_READY_TIME,
    )
    if not latest_closed:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative closed trading session is unavailable"
        )
    latest_closed = _canonical_date(latest_closed)
    if not requested:
        return latest_closed

    target = _canonical_date(requested)
    if target > latest_closed:
        raise RuntimeError(
            "DATA_BLOCKED: requested capital-flow date is not close-ready"
        )
    rows = _read_rows(
        engine,
        """
        SELECT trade_date
          FROM si_trade_calendar
         WHERE trade_date=:trade_date AND trade_status=1
        """,
        {"trade_date": target},
    )
    if len(rows) != 1:
        raise RuntimeError(
            "DATA_BLOCKED: requested capital-flow date is not an open session"
        )
    return target


def sign_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("receipt_id", None)
    result["receipt_id"] = _sha256_json(result)
    return result


def build_receipt(
    engine: object,
    trade_date: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    evidence = inspect_partition(engine, trade_date)
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    verified_at = current.replace(microsecond=0).isoformat(timespec="seconds")
    return sign_receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "PASS",
            "task_type": TASK_TYPE,
            "dataset": DATASET,
            "provider": PROVIDER,
            "build_sha": _scheduler_build_sha(),
            "trade_date": evidence["trade_date"],
            "source_trade_date": evidence["trade_date"],
            "row_count": evidence["row_count"],
            "expected_row_count": evidence["expected_row_count"],
            "code_set_sha256": evidence["code_set_sha256"],
            "partition_sha256": evidence["partition_sha256"],
            "source_counts": evidence["source_counts"],
            "verification_mode": "direct_qmt_persisted_read_only",
            "read_only": True,
            "network_accessed": False,
            "verified_at": verified_at,
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify the exact persisted direct-QMT daily-flow partition"
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    engine = create_batch_engine()
    try:
        target = resolve_trade_date(engine, args.trade_date)
        payload = build_receipt(engine, target)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:  # scheduler needs one bounded machine failure
        payload = {
            "schema": RECEIPT_SCHEMA,
            "status": "DATA_BLOCKED",
            "error": str(exc)[:500],
            "generated_at": datetime.now(PRODUCTION_TIMEZONE)
            .replace(tzinfo=None, microsecond=0)
            .isoformat(timespec="seconds"),
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
