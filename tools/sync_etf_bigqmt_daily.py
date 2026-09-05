#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish one complete, build-bound BigQMT ETF daily partition.

The publisher owns a frozen 14-fund research universe.  It fetches the exact
same target date twice from the loaded BigQMT model (native/unadjusted and
forward-adjusted), validates all 28 bars before DML, and replaces both
``sm_etf_kline`` partitions in one transaction.  It never creates or alters
runtime schema and never submits orders.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt import bridge as bigqmt_bridge
from integrations.bigqmt.release_identity import validate_strategy_release_payload
from tools.env_config import create_tool_engine, load_project_env


SHANGHAI = ZoneInfo("Asia/Shanghai")
PROVIDER_ID = "gj_big_qmt_inner"
BRIDGE_VERSION = "bigqmt_inner_v2"
RECEIPT_SCHEMA = "probiga.etf-bigqmt-daily-receipt.v1"
STRATEGY_SOURCE = (
    ROOT
    / "integrations"
    / "bigqmt"
    / "qmt_strategy"
    / "probiga_big_qmt_bridge.py"
)


@dataclass(frozen=True)
class ETFMeta:
    code: str
    short_name: str
    asset_class: str


# This is the reviewed research-data universe from the original ETF forward
# protocol.  Changing it requires a new task/research contract, not an ambient
# database row or provider response.
ETF_UNIVERSE: tuple[ETFMeta, ...] = (
    ETFMeta("510300", "沪深300ETF", "A股宽基"),
    ETFMeta("510500", "中证500ETF", "A股宽基"),
    ETFMeta("159915", "创业板ETF", "A股宽基"),
    ETFMeta("512100", "中证1000ETF", "A股宽基"),
    ETFMeta("510880", "红利ETF", "A股红利"),
    ETFMeta("512890", "红利低波ETF", "A股红利"),
    ETFMeta("518880", "黄金ETF", "商品"),
    ETFMeta("159985", "豆粕ETF", "商品"),
    ETFMeta("513100", "纳指ETF", "海外权益"),
    ETFMeta("513500", "标普500ETF", "海外权益"),
    ETFMeta("510900", "H股ETF", "港股权益"),
    ETFMeta("511010", "国债ETF", "债券"),
    ETFMeta("511380", "可转债ETF", "债券"),
    ETFMeta("511880", "银华日利ETF", "现金管理"),
)
ETF_CODES: tuple[str, ...] = tuple(sorted(meta.code for meta in ETF_UNIVERSE))
ETF_BY_CODE = {meta.code: meta for meta in ETF_UNIVERSE}
ADJUSTMENTS: tuple[tuple[str, int], ...] = (("none", 0), ("front", 1))

ETF_KLINE_COLUMNS = (
    "etf_code",
    "short_name",
    "trade_time",
    "trade_date",
    "k_type",
    "adjust_type",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "pre_close",
    "change",
    "change_pct",
    "data_source",
    "validation_source",
    "validation_status",
    "validation_price_max_delta",
    "validation_volume_delta_pct",
    "validation_checked_at",
    "received_at",
    "batch_id",
    "data_version",
    "quality_status",
    "permission_status",
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def code_set_hash(codes: Iterable[str]) -> str:
    normalized = sorted({str(code).strip().zfill(6) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def etf_qmt_symbol(code: str) -> str:
    normalized = str(code or "").strip().zfill(6)
    if len(normalized) != 6 or not normalized.isdigit():
        raise RuntimeError(f"invalid ETF code: {code!r}")
    if normalized.startswith("5"):
        return f"{normalized}.SH"
    if normalized.startswith("1"):
        return f"{normalized}.SZ"
    raise RuntimeError(f"unsupported ETF exchange prefix: {code!r}")


ETF_QMT_SYMBOLS: tuple[str, ...] = tuple(etf_qmt_symbol(code) for code in ETF_CODES)


def _exact_git_sha(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if (
        len(normalized) != 40
        or normalized == "0" * 40
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise RuntimeError("ETF publisher requires one nonzero exact build SHA")
    return normalized


def _git_head() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=True,
        timeout=30,
    )
    return _exact_git_sha(result.stdout)


def resolve_expected_build_sha(explicit: str = "") -> str:
    expected = _exact_git_sha(
        explicit or os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "") or _git_head()
    )
    if _git_head() != expected:
        raise RuntimeError("ETF publisher checkout differs from expected build SHA")
    environment_sha = str(
        os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or ""
    ).strip()
    if environment_sha and _exact_git_sha(environment_sha) != expected:
        raise RuntimeError("ETF publisher environment build SHA differs")
    return expected


def _iso_date(value: Any, *, field: str) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"ETF {field} is not a date") from exc
    if pd.isna(parsed):
        raise RuntimeError(f"ETF {field} is not a date")
    return parsed.date().isoformat()


def _naive_datetime(value: Any, *, field: str) -> datetime:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"ETF {field} is not a datetime") from exc
    if pd.isna(parsed):
        raise RuntimeError(f"ETF {field} is not a datetime")
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert(SHANGHAI).tz_localize(None)
    return parsed.to_pydatetime().replace(microsecond=0)


def _decimal(
    value: Any,
    *,
    field: str,
    positive: bool = False,
    scale: int = 6,
    allow_negative: bool = False,
) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError(f"ETF {field} is not numeric") from exc
    if not number.is_finite() or (not allow_negative and number < 0) or (positive and number <= 0):
        relation = "positive" if positive else "nonnegative"
        raise RuntimeError(f"ETF {field} must be finite and {relation}")
    quantum = Decimal(1).scaleb(-scale)
    return number.quantize(quantum, rounding=ROUND_HALF_UP)


def _format_decimal(value: Any, scale: int, *, allow_negative: bool = False) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return format(_decimal(value, field="hash numeric", scale=scale, allow_negative=allow_negative), f".{scale}f")


def _required_runtime_columns() -> dict[str, frozenset[str]]:
    return {
        "sm_etf_kline": frozenset({"id", *ETF_KLINE_COLUMNS}),
        "si_etf_code": frozenset(
            {
                "etf_code",
                "short_name",
                "status",
                "primary_source",
                "sync_status",
            }
        ),
        "si_trade_calendar": frozenset({"trade_date", "trade_status"}),
    }


def validate_runtime_schema(engine: Any) -> dict[str, Any]:
    """Validate the pre-migrated contract without issuing any DDL."""

    required = _required_runtime_columns()
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT TABLE_NAME, COLUMN_NAME
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME IN
                       ('sm_etf_kline','si_etf_code','si_trade_calendar')
                """
            )
        ).fetchall()
        indexes = connection.execute(
            text(
                """
                SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
                  FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME = 'sm_etf_kline'
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """
            )
        ).mappings().all()
    observed: dict[str, set[str]] = {table: set() for table in required}
    for table_name, column_name in rows:
        observed.setdefault(str(table_name), set()).add(str(column_name))
    missing = {
        table: sorted(columns - observed.get(table, set()))
        for table, columns in required.items()
        if columns - observed.get(table, set())
    }
    if missing:
        raise RuntimeError(
            "ETF runtime schema migration is missing: " + _canonical_json(missing)
        )
    by_index: dict[str, list[tuple[int, str, int]]] = {}
    for row in indexes:
        by_index.setdefault(str(row["INDEX_NAME"]), []).append(
            (
                int(row["SEQ_IN_INDEX"]),
                str(row["COLUMN_NAME"]),
                int(row["NON_UNIQUE"]),
            )
        )
    expected_key = ("etf_code", "trade_date", "k_type", "adjust_type")
    unique_keys = {
        tuple(item[1] for item in sorted(values))
        for values in by_index.values()
        if values and all(item[2] == 0 for item in values)
    }
    if expected_key not in unique_keys:
        raise RuntimeError("ETF runtime schema lacks the exact daily-bar unique key")
    return {
        "status": "PASS",
        "tables": sorted(required),
        "schema_hash": _digest(
            {
                table: sorted(observed[table])
                for table in sorted(observed)
            }
        ),
    }


def validate_reference_universe(engine: Any) -> dict[str, Any]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT etf_code, short_name, status, primary_source, sync_status
                  FROM si_etf_code
                 WHERE etf_code IN
                       ('159915','159985','510300','510500','510880','510900',
                        '511010','511380','511880','512100','512890','513100',
                        '513500','518880')
                 ORDER BY etf_code
                """
            )
        ).mappings().all()
    actual = [str(row["etf_code"]).strip().zfill(6) for row in rows]
    if actual != list(ETF_CODES):
        raise RuntimeError(
            "ETF reference universe differs from the frozen 14-code contract"
        )
    inactive = [
        code
        for code, row in zip(actual, rows)
        if str(row.get("status") or "").strip().lower() != "active"
    ]
    if inactive:
        raise RuntimeError(f"ETF reference universe contains inactive codes: {inactive}")
    invalid_provenance = [
        code
        for code, row in zip(actual, rows)
        if str(row.get("primary_source") or "") != PROVIDER_ID
        or str(row.get("sync_status") or "").strip().lower() != "validated"
    ]
    if invalid_provenance:
        raise RuntimeError(
            "ETF reference universe lacks validated BigQMT provenance: "
            f"{invalid_provenance}"
        )
    return {
        "count": len(actual),
        "code_set_hash": code_set_hash(actual),
    }


def _is_trade_day(engine: Any, trade_date: str) -> bool:
    with engine.connect() as connection:
        count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM si_trade_calendar
                 WHERE trade_date=:trade_date AND trade_status=1
                """
            ),
            {"trade_date": trade_date},
        ).scalar()
    return int(count or 0) == 1


_IDENTITY_FIELDS = (
    "strategy_release_protocol",
    "strategy_identity_protocol",
    "strategy_identity_frozen",
    "strategy_build_sha",
    "strategy_git_blob",
    "strategy_source_sha256",
    "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256",
)


def validate_capture_identity(
    capture: Mapping[str, Any],
    release_proof: Mapping[str, Any],
) -> None:
    if (
        capture.get("status") != "ok"
        or capture.get("action") != "kline"
        or capture.get("source") != PROVIDER_ID
        or capture.get("bridge_version") != BRIDGE_VERSION
        or not str(capture.get("request_id") or "").strip()
    ):
        raise RuntimeError("ETF BigQMT response provenance is incomplete")
    for field in _IDENTITY_FIELDS:
        if capture.get(field) != release_proof.get(field):
            raise RuntimeError(
                f"ETF BigQMT response release identity differs: {field}"
            )


def _normalized_source_rows(
    capture: Mapping[str, Any],
    *,
    release_proof: Mapping[str, Any],
    trade_date: str,
    dividend_type: str,
    adjust_type: int,
    batch_id: str,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    validate_capture_identity(capture, release_proof)
    source_rows = capture.get("rows")
    if not isinstance(source_rows, list):
        raise RuntimeError("ETF BigQMT response rows are not a list")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for source in source_rows:
        if not isinstance(source, Mapping):
            raise RuntimeError("ETF BigQMT response contains a non-object row")
        raw_code = str(source.get("stock_code") or "").strip()
        if not raw_code:
            raise RuntimeError("ETF BigQMT response contains an empty code")
        code = raw_code.zfill(6)
        if code in seen:
            raise RuntimeError(f"ETF BigQMT response duplicates code {code}")
        seen.add(code)
        if code not in ETF_BY_CODE:
            raise RuntimeError(f"ETF BigQMT response contains unexpected code {code}")
        expected_symbol = etf_qmt_symbol(code)
        if str(source.get("qmt_code") or "").strip().upper() != expected_symbol:
            raise RuntimeError(f"ETF BigQMT symbol differs for {code}")
        source_date = _iso_date(source.get("trade_date"), field="trade_date")
        if source_date != trade_date:
            raise RuntimeError(
                f"ETF BigQMT date differs for {code}: {source_date} != {trade_date}"
            )
        trade_time = _naive_datetime(
            source.get("trade_time") or f"{trade_date} 15:00:00",
            field="trade_time",
        )
        if trade_time.date().isoformat() != trade_date:
            raise RuntimeError(f"ETF BigQMT trade_time differs for {code}")
        open_price = _decimal(source.get("open"), field=f"{code}.open", positive=True)
        close_price = _decimal(source.get("close"), field=f"{code}.close", positive=True)
        high_price = _decimal(source.get("high"), field=f"{code}.high", positive=True)
        low_price = _decimal(source.get("low"), field=f"{code}.low", positive=True)
        pre_close = _decimal(
            source.get("pre_close"),
            field=f"{code}.pre_close",
            positive=True,
        )
        if str(source.get("pre_close_origin") or "") != "NATIVE_QMT":
            raise RuntimeError(f"ETF BigQMT lacks native pre_close for {code}")
        if high_price < max(open_price, close_price, low_price):
            raise RuntimeError(f"ETF BigQMT high is invalid for {code}")
        if low_price > min(open_price, close_price, high_price):
            raise RuntimeError(f"ETF BigQMT low is invalid for {code}")
        # BigQMT's native daily API reports volume in lots; the canonical ETF
        # table stores shares, matching the rest of ProBigA's daily K-lines.
        volume_lots = _decimal(
            source.get("volume"), field=f"{code}.volume", positive=True, scale=4
        )
        volume = (volume_lots * Decimal(100)).quantize(Decimal("0.0001"))
        amount = _decimal(source.get("amount"), field=f"{code}.amount", scale=4)
        change = (close_price - pre_close).quantize(Decimal("0.000001"))
        change_pct = (
            change / pre_close * Decimal(100)
        ).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        market_identity = {
            "etf_code": code,
            "trade_date": trade_date,
            "adjust_type": adjust_type,
            "dividend_type": dividend_type,
            "open": format(open_price, "f"),
            "close": format(close_price, "f"),
            "high": format(high_price, "f"),
            "low": format(low_price, "f"),
            "volume": format(volume, "f"),
            "amount": format(amount, "f"),
            "pre_close": format(pre_close, "f"),
            "source": PROVIDER_ID,
            "strategy_loaded_identity_sha256": release_proof[
                "strategy_loaded_identity_sha256"
            ],
        }
        normalized.append(
            {
                "etf_code": code,
                "short_name": ETF_BY_CODE[code].short_name,
                "trade_time": trade_time,
                "trade_date": date.fromisoformat(trade_date),
                "k_type": 1,
                "adjust_type": adjust_type,
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "amount": amount,
                "pre_close": pre_close,
                "change": change,
                "change_pct": change_pct,
                "data_source": PROVIDER_ID,
                "validation_source": "bigqmt_identity_and_set",
                "validation_status": "passed",
                # No independent price vendor is used in this formal path.
                # Null deltas avoid claiming a cross-vendor equality that was
                # not observed; the loaded release, exact sets, dates and
                # native pre-close fields are the validated facts.
                "validation_price_max_delta": None,
                "validation_volume_delta_pct": None,
                "validation_checked_at": observed_at.replace(tzinfo=None),
                "received_at": observed_at.replace(tzinfo=None),
                "batch_id": batch_id,
                "data_version": _digest(market_identity),
                "quality_status": "validated",
                "permission_status": "SUPPORTED",
            }
        )
    if seen != set(ETF_CODES):
        raise RuntimeError(
            "ETF BigQMT response code set differs: "
            f"expected={len(ETF_CODES)}, responded={len(seen)}, "
            f"missing={sorted(set(ETF_CODES) - seen)}"
        )
    return sorted(normalized, key=lambda row: str(row["etf_code"]))


def _canonical_partition_row(row: Mapping[str, Any]) -> dict[str, Any]:
    trade_time = _naive_datetime(row.get("trade_time"), field="db.trade_time")
    return {
        "etf_code": str(row.get("etf_code") or "").strip().zfill(6),
        "trade_time": trade_time.isoformat(sep=" ", timespec="seconds"),
        "trade_date": _iso_date(row.get("trade_date"), field="db.trade_date"),
        "k_type": int(row.get("k_type") or 0),
        "adjust_type": int(row.get("adjust_type")),
        "open": _format_decimal(row.get("open"), 6),
        "close": _format_decimal(row.get("close"), 6),
        "high": _format_decimal(row.get("high"), 6),
        "low": _format_decimal(row.get("low"), 6),
        "volume": _format_decimal(row.get("volume"), 4),
        "amount": _format_decimal(row.get("amount"), 4),
        "pre_close": _format_decimal(row.get("pre_close"), 6),
        "change": _format_decimal(row.get("change"), 6, allow_negative=True),
        "change_pct": _format_decimal(row.get("change_pct"), 8, allow_negative=True),
        "data_source": str(row.get("data_source") or ""),
        "validation_source": str(row.get("validation_source") or ""),
        "validation_status": str(row.get("validation_status") or ""),
        "batch_id": str(row.get("batch_id") or ""),
        "data_version": str(row.get("data_version") or ""),
        "quality_status": str(row.get("quality_status") or ""),
        "permission_status": str(row.get("permission_status") or ""),
    }


def validate_partition_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    trade_date: str,
) -> dict[str, Any]:
    canonical = sorted(
        (_canonical_partition_row(row) for row in rows),
        key=lambda row: (row["adjust_type"], row["etf_code"]),
    )
    if len(canonical) != len(ETF_CODES) * len(ADJUSTMENTS):
        raise RuntimeError(
            f"ETF database partition row count differs: {len(canonical)}"
        )
    group_hashes: dict[str, str] = {}
    for dividend_type, adjust_type in ADJUSTMENTS:
        group = [row for row in canonical if row["adjust_type"] == adjust_type]
        codes = [row["etf_code"] for row in group]
        if codes != list(ETF_CODES) or len(set(codes)) != len(codes):
            raise RuntimeError(
                f"ETF database {dividend_type} code set differs from frozen universe"
            )
        for row in group:
            if (
                row["trade_date"] != trade_date
                or row["k_type"] != 1
                or row["data_source"] != PROVIDER_ID
                or row["validation_status"] != "passed"
                or row["quality_status"] != "validated"
                or row["permission_status"] != "SUPPORTED"
                or len(row["data_version"]) != 64
            ):
                raise RuntimeError(
                    f"ETF database {dividend_type} row contract differs"
                )
        group_hashes[dividend_type] = _digest(group)
    return {
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
        "group_hashes": group_hashes,
    }


_INSERT_SQL = text(
    """
    INSERT INTO sm_etf_kline
      (etf_code,short_name,trade_time,trade_date,k_type,adjust_type,
       `open`,`close`,high,low,volume,amount,pre_close,`change`,change_pct,
       data_source,validation_source,validation_status,
       validation_price_max_delta,validation_volume_delta_pct,
       validation_checked_at,received_at,batch_id,data_version,
       quality_status,permission_status)
    VALUES
      (:etf_code,:short_name,:trade_time,:trade_date,:k_type,:adjust_type,
       :open,:close,:high,:low,:volume,:amount,:pre_close,:change,:change_pct,
       :data_source,:validation_source,:validation_status,
       :validation_price_max_delta,:validation_volume_delta_pct,
       :validation_checked_at,:received_at,:batch_id,:data_version,
       :quality_status,:permission_status)
    """
)

_READBACK_SQL = text(
    """
    SELECT etf_code,trade_time,trade_date,k_type,adjust_type,
           `open`,`close`,high,low,volume,amount,pre_close,`change`,change_pct,
           data_source,validation_source,validation_status,batch_id,data_version,
           quality_status,permission_status
      FROM sm_etf_kline
     WHERE trade_date=:trade_date
       AND k_type=1
       AND adjust_type IN (0,1)
     ORDER BY adjust_type,etf_code
    """
)


def replace_daily_partition(
    engine: Any,
    *,
    trade_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Atomically replace both complete adjustment groups and prove readback."""

    expected = validate_partition_rows(rows, trade_date=trade_date)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM sm_etf_kline
                 WHERE trade_date=:trade_date
                   AND k_type=1
                   AND adjust_type IN (0,1)
                """
            ),
            {"trade_date": trade_date},
        )
        connection.execute(_INSERT_SQL, rows)
        transaction_rows = connection.execute(
            _READBACK_SQL, {"trade_date": trade_date}
        ).mappings().all()
        transaction_proof = validate_partition_rows(
            transaction_rows, trade_date=trade_date
        )
        if transaction_proof != expected:
            raise RuntimeError("ETF transaction readback hash differs before commit")
    with engine.connect() as connection:
        committed_rows = connection.execute(
            _READBACK_SQL, {"trade_date": trade_date}
        ).mappings().all()
    committed = validate_partition_rows(committed_rows, trade_date=trade_date)
    if committed != expected:
        raise RuntimeError("ETF committed partition readback hash differs")
    return committed


def _release_summary(release_proof: Mapping[str, Any]) -> dict[str, Any]:
    return {field: release_proof.get(field) for field in _IDENTITY_FIELDS}


def _receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _digest(result)
    return result


def run_sync(
    engine: Any,
    *,
    trade_date: str,
    expected_build_sha: str,
    now: datetime | None = None,
    capabilities_runner: Callable[..., dict[str, Any]] = bigqmt_bridge.capabilities,
    capture_runner: Callable[..., dict[str, Any]] = bigqmt_bridge.kline_capture,
) -> dict[str, Any]:
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    target = date.fromisoformat(trade_date)
    if target > current.date():
        raise RuntimeError("ETF target date cannot be in the future")
    if target == current.date() and current.hour * 100 + current.minute < 1505:
        raise RuntimeError("ETF current-day partition may publish only after 15:05")

    schema = validate_runtime_schema(engine)
    reference = validate_reference_universe(engine)
    if not _is_trade_day(engine, trade_date):
        return _receipt(
            {
                "schema": RECEIPT_SCHEMA,
                "status": "SKIPPED_NON_TRADE_DAY",
                "trade_date": trade_date,
                "provider": PROVIDER_ID,
                "executor_owner": "qmt_windows_edge",
                "schema_hash": schema["schema_hash"],
                "universe": reference,
                "automatic_order_submission": False,
            }
        )

    capabilities = capabilities_runner(timeout=180)
    release_proof = validate_strategy_release_payload(
        capabilities,
        expected_build_sha=expected_build_sha,
        root=ROOT,
        source_path=STRATEGY_SOURCE,
    )
    captures: dict[str, dict[str, Any]] = {}
    for dividend_type, _adjust_type in ADJUSTMENTS:
        captures[dividend_type] = capture_runner(
            ETF_QMT_SYMBOLS,
            start_date=trade_date,
            end_date=trade_date,
            dividend_type=dividend_type,
            download_history=True,
            batch_size=len(ETF_CODES),
            timeout=300,
        )
    batch_id = (
        f"etf_{target.strftime('%Y%m%d')}_"
        + _digest(
            {
                "trade_date": trade_date,
                "build_sha": expected_build_sha,
                "requests": [
                    captures[dividend_type].get("request_id")
                    for dividend_type, _adjust_type in ADJUSTMENTS
                ],
            }
        )[:32]
    )
    observed_at = current.replace(microsecond=0)
    rows: list[dict[str, Any]] = []
    group_receipts: dict[str, Any] = {}
    for dividend_type, adjust_type in ADJUSTMENTS:
        group = _normalized_source_rows(
            captures[dividend_type],
            release_proof=release_proof,
            trade_date=trade_date,
            dividend_type=dividend_type,
            adjust_type=adjust_type,
            batch_id=batch_id,
            observed_at=observed_at,
        )
        rows.extend(group)
        group_receipts[dividend_type] = {
            "adjust_type": adjust_type,
            "request_id": captures[dividend_type]["request_id"],
            "requested_code_count": len(ETF_CODES),
            "requested_code_set_hash": code_set_hash(ETF_CODES),
            "responded_code_count": len(group),
            "responded_code_set_hash": code_set_hash(
                row["etf_code"] for row in group
            ),
            "source_row_hash": _digest(
                [_canonical_partition_row(row) for row in group]
            ),
        }
    committed = replace_daily_partition(
        engine,
        trade_date=trade_date,
        rows=rows,
    )
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "PASS",
            "trade_date": trade_date,
            "provider": PROVIDER_ID,
            "executor_owner": "qmt_windows_edge",
            "batch_id": batch_id,
            "groups": group_receipts,
            "database": committed,
            "schema_hash": schema["schema_hash"],
            "universe": reference,
            "source_identity": _release_summary(release_proof),
            "automatic_order_submission": False,
        }
    )


def _failure_receipt(*, trade_date: str, error: BaseException) -> dict[str, Any]:
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "DATA_BLOCKED",
            "trade_date": trade_date,
            "provider": PROVIDER_ID,
            "executor_owner": "qmt_windows_edge",
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
            "automatic_order_submission": False,
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--expected-build-sha", default="")
    args = parser.parse_args(argv)
    now = datetime.now(SHANGHAI)
    trade_date = args.trade_date or now.date().isoformat()
    try:
        date.fromisoformat(trade_date)
        if not args.execute:
            result = _receipt(
                {
                    "schema": RECEIPT_SCHEMA,
                    "status": "DRY_RUN",
                    "trade_date": trade_date,
                    "provider": PROVIDER_ID,
                    "executor_owner": "qmt_windows_edge",
                    "universe": {
                        "count": len(ETF_CODES),
                        "code_set_hash": code_set_hash(ETF_CODES),
                    },
                    "automatic_order_submission": False,
                }
            )
            print(_canonical_json(result), flush=True)
            return 0
        load_project_env()
        expected_build_sha = resolve_expected_build_sha(args.expected_build_sha)
        engine = create_tool_engine()
        try:
            result = run_sync(
                engine,
                trade_date=trade_date,
                expected_build_sha=expected_build_sha,
                now=now,
            )
        finally:
            engine.dispose()
    except Exception as exc:  # one fail-closed machine receipt, no partial DML
        result = _failure_receipt(trade_date=trade_date, error=exc)
        print(_canonical_json(result), flush=True)
        return 1
    print(_canonical_json(result), flush=True)
    return 0 if result["status"] in {"PASS", "SKIPPED_NON_TRADE_DAY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
