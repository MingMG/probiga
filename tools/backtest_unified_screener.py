#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only forward-return backtest for the unified screener presets.

Returns are chained with each session's official ``pre_close`` reference
instead of raw cross-day close ratios. This avoids false jumps caused by
ex-right/ex-dividend price discontinuities while preserving the actual
T+1-open entry price.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers.screener import PRESETS
from server.common.batch_db import create_batch_engine, read_frame_direct
from server.common.kline_data import get_kline_engine
from server.common.pit_execution_guard import (
    daily_bar_execution_disposition,
    nonlinear_impact_rate,
    participation_capped_quantity,
    validate_open_execution_receipt,
)
from server.common.qmt_attestation_contract import ATTESTATION_PROTOCOL_VERSION
from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from server.engine.strategy_statistical_guards import (
    benjamini_yekutieli_fdr,
    newey_west_nav_statistics,
)
from tools.audit_screener_input_range import audit_inputs

HORIZONS = (1, 5, 20)
RELEASE_THRESHOLDS = {
    "minimum_universe_coverage": 0.95,
    "minimum_mature_samples_per_horizon": 80,
    "minimum_oos_profit_factor": 1.30,
    "minimum_oos_average_win_loss": 1.0,
    "minimum_shadow_sessions": 20,
    "maximum_data_missing_rate": 0.05,
    "minimum_execution_disposition_coverage": 1.0,
    "maximum_execution_data_blocked": 0,
    "maximum_hypothetical_only": 0,
}

DEFAULT_MAXIMUM_PARTICIPATION_RATE = 0.05
DEFAULT_IMPACT_BASE_RATE = 0.001
DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY = 1_000_000.0
DEFAULT_MAXIMUM_CONCURRENT_POSITIONS = 10
DEFAULT_MAXIMUM_STOCK_WEIGHT = 0.10
DEFAULT_MAXIMUM_INDUSTRY_WEIGHT = 0.25
DEFAULT_ADV_LOOKBACK_SESSIONS = 20
DEFAULT_MINIMUM_ADV_SESSIONS = 5
SCREENER_RECEIPT_SCHEMA = "probiga.unified-screener-run-receipt.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACCEPTED_FRESHNESS = frozenset({"exact", "live", "historical_close"})
_COMBINED_TRIAL_KEY = "__combined__"
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _market_truth_window(
    start_date: str,
    end_date: str,
    *,
    decision_known_at: datetime,
):
    with create_batch_engine().connect() as conn:
        daily_truth = load_qmt_daily_market_truth(
            conn,
            start_date=start_date,
            end_date=end_date,
            decision_known_at=decision_known_at,
        )
        calendar = load_trade_calendar_receipt(
            conn,
            start_date=start_date,
            end_date=end_date,
            decision_known_at=decision_known_at,
            batch_id=daily_truth.calendar_batch_id,
        )
        catalog = load_stock_catalog(
            conn,
            decision_known_at=decision_known_at,
            batch_id=daily_truth.catalog_batch_id,
        )
    if start_date < catalog.history_complete_from:
        raise RuntimeError(
            "QMT stock catalog does not prove the unified backtest window"
        )
    if (
        catalog.manifest_hash != daily_truth.catalog_manifest_hash
        or calendar.manifest_hash != daily_truth.calendar_manifest_hash
    ):
        raise RuntimeError("QMT unified backtest market roots differ")
    return calendar, catalog, daily_truth


def _load_prices(
    start_date: str,
    end_date: str,
    *,
    catalog_batch_id: str,
    selected_run_id: str,
    run_finished_at: str,
) -> pd.DataFrame:
    frame = read_frame_direct(text("""
            SELECT
              k.stock_code, k.short_name, k.trade_date,
              k.`open`, k.`high`, k.`low`, k.`close`,
              k.volume, k.amount, k.pre_close, k.change_pct
            FROM sm_stock_kline AS k
            JOIN qmt_stock_catalog_member AS member
              ON member.batch_id = :catalog_batch_id
             AND member.stock_code = LEFT(k.stock_code, 6)
             AND member.instrument_type = 'STOCK'
             AND member.list_date <= k.trade_date
             AND (member.expire_date IS NULL OR member.expire_date >= k.trade_date)
            WHERE k.trade_date BETWEEN :start_date AND :end_date
              AND k.k_type = 1
              AND k.adjust_type = 0
              AND EXISTS (
                  SELECT 1 FROM qmt_kline_attestation_row AS attestation
                  WHERE attestation.target_id=k.id
                    AND BINARY attestation.run_id=BINARY :selected_run_id
                    AND BINARY attestation.protocol_version=
                        BINARY :protocol_version
                    AND attestation.created_at<=:run_finished_at
                    AND BINARY attestation.source_data_version=
                        BINARY k.data_version
                    AND BINARY attestation.source_pre_close_origin=
                        BINARY 'NATIVE_QMT'
                    AND attestation.trade_date=k.trade_date
                    AND attestation.stock_code=LEFT(k.stock_code, 6)
                    AND attestation.source_pre_close=k.pre_close
                    AND attestation.attested_open=k.`open`
                    AND attestation.attested_close=k.`close`
                    AND attestation.attested_high=k.high
                    AND attestation.attested_low=k.low
                    AND attestation.attested_volume=k.volume
                    AND attestation.attested_amount=k.amount
              )
            ORDER BY k.trade_date, k.stock_code
        """), get_kline_engine(), params={
            "start_date": start_date,
            "end_date": end_date,
            "catalog_batch_id": catalog_batch_id,
            "protocol_version": ATTESTATION_PROTOCOL_VERSION,
            "selected_run_id": selected_run_id,
            "run_finished_at": run_finished_at,
        })
    if frame.empty:
        return frame
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    for column in ("open", "high", "low", "close", "volume", "amount", "pre_close", "change_pct"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _catalog_coverage_failures(
    frame: pd.DataFrame,
    *,
    trade_dates: list[str],
    catalog,
) -> dict[str, dict[str, Any]]:
    observed_by_date = {
        str(day): set(group["stock_code"].astype(str))
        for day, group in frame.groupby("trade_date", sort=False)
    } if not frame.empty else {}
    failures: dict[str, dict[str, Any]] = {}
    for day in trade_dates:
        expected = set(catalog.eligible_codes(day))
        observed = observed_by_date.get(day, set())
        if observed != expected:
            failures[day] = {
                "expected_count": len(expected),
                "observed_count": len(observed),
                "missing": sorted(expected - observed)[:20],
                "extra": sorted(observed - expected)[:20],
            }
    return failures


def _dataset_hash(frame: pd.DataFrame) -> str:
    columns = [
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "amount", "pre_close",
    ]
    payload = frame[columns].to_json(orient="records", double_precision=10)
    return hashlib.sha256(payload.encode()).hexdigest()


def _data_audit(frame: pd.DataFrame, expected_dates: list[str]) -> dict[str, Any]:
    actual_dates = sorted(frame["trade_date"].unique().tolist()) if not frame.empty else []
    duplicate_count = int(frame.duplicated(["stock_code", "trade_date"]).sum()) if not frame.empty else 0
    if frame.empty:
        bad_ohlc = 0
        invalid_price = 0
        missing_pre_close = 0
        inconsistent_reference_returns = 0
    else:
        bad_mask = (
            frame["high"].lt(frame[["open", "low", "close"]].max(axis=1))
            | frame["low"].gt(frame[["open", "high", "close"]].min(axis=1))
        )
        bad_ohlc = int(bad_mask.sum())
        invalid_price = int(
            (frame[["open", "high", "low", "close"]].isna().any(axis=1)
             | frame[["open", "high", "low", "close"]].le(0).any(axis=1)).sum()
        )
        missing_pre_close = int((frame["pre_close"].isna() | frame["pre_close"].le(0)).sum())
        reference_mask = (
            frame["pre_close"].notna()
            & frame["pre_close"].gt(0)
            & frame["close"].notna()
            & frame["change_pct"].notna()
        )
        reference_delta = (
            (frame.loc[reference_mask, "close"] / frame.loc[reference_mask, "pre_close"] - 1.0) * 100.0
            - frame.loc[reference_mask, "change_pct"]
        ).abs()
        inconsistent_reference_returns = int(reference_delta.gt(0.05).sum())
    return {
        "expected_trade_dates": expected_dates,
        "actual_trade_dates": actual_dates,
        "missing_trade_dates": sorted(set(expected_dates) - set(actual_dates)),
        "row_count": len(frame),
        "stock_count": int(frame["stock_code"].nunique()) if not frame.empty else 0,
        "duplicate_business_keys": duplicate_count,
        "bad_ohlc": bad_ohlc,
        "invalid_prices": invalid_price,
        "missing_pre_close_rows": missing_pre_close,
        "inconsistent_reference_return_rows": inconsistent_reference_returns,
        "dataset_sha256": _dataset_hash(frame) if not frame.empty else "",
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _exact_timestamp(value: Any) -> datetime:
    raw = str(value or "").strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    if parsed.microsecond:
        raise ValueError("timestamp must have exact whole-second precision")
    return parsed


def _finite(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} is boolean")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field} is not finite")
    return result


def _candidate_industry(row: Mapping[str, Any]) -> str:
    for field in (
        "sw_industry_code", "industry_code", "primary_industry_code",
        "sw_industry_name", "industry_name", "primary_industry",
    ):
        value = str(row.get(field) or "").strip()
        if value:
            return value[:120]
    return ""


def _source_screener_run_key(
    request_payload: Mapping[str, Any],
    result: Mapping[str, Any],
) -> str:
    """Reproduce the existing persisted screener run content commitment."""

    rows = result.get("data") or []
    signature = {
        "request": dict(request_payload),
        "data_date": result.get("data_date"),
        "evidence_date": result.get("evidence_date"),
        "observed_at": result.get("observed_at"),
        "freshness": result.get("freshness"),
        "selector": (result.get("selector") or {}).get("model_fingerprint"),
        "results": [
            [
                row.get("rank"),
                row.get("stock_code"),
                row.get("ensemble_score", row.get("score")),
            ]
            for row in rows
        ],
    }
    raw = json.dumps(
        signature,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _freeze_screener_run_receipt(
    *,
    target_date: str,
    preset: str,
    result: Mapping[str, Any],
    source_run_uid: str,
    source_run_key: str,
    source_generated_at: Any,
    request_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze and validate one exact server run before it can create signals.

    This is the minimum application-layer receipt for the legacy screener
    tables.  It binds the pre-existing run key, every returned candidate, the
    exact server decision timestamp and the immutable common PIT fact root.
    Runtime code is strictly read-only and never creates or upgrades tables.
    """

    failure = ""
    normalized_rows: list[dict[str, Any]] = []
    decision_at = ""
    fact_root = ""
    try:
        day = date.fromisoformat(str(target_date)).isoformat()
        key = str(preset or "").strip()
        run_uid = str(source_run_uid or "").strip().lower()
        run_key = str(source_run_key or "").strip().lower()
        generated = _exact_timestamp(source_generated_at)
        if not key:
            raise ValueError("preset is missing")
        if not re.fullmatch(r"[0-9a-f]{32}", run_uid):
            raise ValueError("persisted run uid is invalid")
        if not _SHA256_RE.fullmatch(run_key):
            raise ValueError("persisted run key is invalid")
        if generated.date().isoformat() != day:
            raise ValueError("historical run was generated after its decision day")
        data_date = str(result.get("data_date") or "")[:10]
        freshness = str(result.get("freshness") or "").strip().lower()
        if data_date != day:
            raise ValueError("screener data date is not exact")
        if freshness not in _ACCEPTED_FRESHNESS:
            raise ValueError("screener freshness is not exact")
        if str(result.get("status") or "").lower() != "ok":
            raise ValueError("screener run status is not ok")
        rows = result.get("data")
        if not isinstance(rows, list):
            raise ValueError("screener candidate payload is not a list")
        if _source_screener_run_key(request_payload, result) != run_key:
            raise ValueError("persisted screener run key does not match payload")

        decision_values: set[str] = set()
        root_values: set[str] = set()
        for index, raw_row in enumerate(rows, 1):
            if not isinstance(raw_row, Mapping):
                raise ValueError("screener candidate is not an object")
            row = dict(raw_row)
            code = str(row.get("stock_code") or "").strip().split(".")[0].zfill(6)
            if not re.fullmatch(r"\d{6}", code):
                raise ValueError("screener candidate stock code is invalid")
            rank = int(row.get("rank") or index)
            if rank != index:
                raise ValueError("screener candidate ranks are not contiguous")
            if row.get("pit_score_binding_verified") is not True:
                raise ValueError("candidate PIT score binding is not verified")
            if row.get("finance_pit_verified") is not True:
                raise ValueError("candidate finance PIT revision is not verified")
            if row.get("event_pit_verified") is not True:
                raise ValueError("candidate event PIT revision is not verified")
            if str(row.get("pit_strategy_status") or "") != "PIT_AVAILABLE":
                raise ValueError("candidate strategy input is PIT_DATA_BLOCKED")
            if str(row.get("pit_common_cutoff_status") or "") != "PIT_AVAILABLE":
                raise ValueError("candidate common PIT cutoff is blocked")
            row_decision = _exact_timestamp(row.get("pit_decision_at"))
            row_cutoff = _exact_timestamp(row.get("pit_fact_cutoff_at"))
            if row_decision.date().isoformat() != day:
                raise ValueError("candidate decision timestamp date differs")
            if row_decision > generated:
                raise ValueError("candidate decision timestamp follows persisted run")
            if row_cutoff > row_decision:
                raise ValueError("candidate fact cutoff follows decision timestamp")
            row_root = str(
                row.get("pit_common_receipt_root_hash") or ""
            ).strip().lower()
            if not _SHA256_RE.fullmatch(row_root):
                raise ValueError("candidate immutable fact root is missing")
            industry = _candidate_industry(row)
            if not industry:
                raise ValueError("candidate PIT industry is missing")
            industry_status = str(
                row.get("industry_evidence_status")
                or row.get("industry_pit_status")
                or ""
            ).strip()
            if industry_status and industry_status != "PIT_AVAILABLE":
                raise ValueError("candidate industry evidence is PIT_DATA_BLOCKED")
            if row.get("industry_pit_verified") is False:
                raise ValueError("candidate industry PIT binding is not verified")
            score_value = row.get("ensemble_score", row.get("score"))
            score = _finite(score_value, field="candidate score")
            decision_iso = row_decision.isoformat(timespec="seconds")
            decision_values.add(decision_iso)
            root_values.add(row_root)
            normalized_rows.append({
                "rank": rank,
                "stock_code": code,
                "stock_name": str(
                    row.get("stock_name") or row.get("short_name") or ""
                )[:120],
                "score": score,
                "industry": industry,
                "industry_evidence_status": industry_status,
                "pit_decision_at": decision_iso,
                "pit_fact_cutoff_at": row_cutoff.isoformat(timespec="seconds"),
                "pit_common_receipt_root_hash": row_root,
                "pit_score_binding_verified": True,
                "finance_pit_verified": True,
                "event_pit_verified": True,
            })
        if not rows:
            top_decision = _exact_timestamp(result.get("pit_decision_at"))
            top_root = str(
                result.get("pit_common_receipt_root_hash") or ""
            ).strip().lower()
            if not _SHA256_RE.fullmatch(top_root):
                raise ValueError("empty run lacks immutable PIT coverage root")
            decision_values.add(top_decision.isoformat(timespec="seconds"))
            root_values.add(top_root)
        if len(decision_values) != 1 or len(root_values) != 1:
            raise ValueError("candidates do not share one decision/fact root")
        decision_at = next(iter(decision_values))
        fact_root = next(iter(root_values))
    except (ArithmeticError, TypeError, ValueError) as exc:
        failure = str(exc)

    payload = {
        "schema": SCREENER_RECEIPT_SCHEMA,
        "target_date": str(target_date),
        "preset": str(preset),
        "source_run_uid": str(source_run_uid or ""),
        "source_run_key": str(source_run_key or ""),
        "source_generated_at": str(source_generated_at or ""),
        "decision_at": decision_at,
        "pit_common_receipt_root_hash": fact_root,
        "candidates": normalized_rows if not failure else [],
        "validation_reason": (
            "SCREENER_RUN_RECEIPT_VERIFIED" if not failure else failure
        ),
        "order_authority": False,
        "automatic_real_order_submission": False,
    }
    return {
        **copy.deepcopy(payload),
        "valid": not failure,
        "reason": "SCREENER_RUN_RECEIPT_VERIFIED" if not failure else failure,
        "receipt_root_hash": _digest(payload),
    }


def _validate_screener_run_receipt(receipt: Mapping[str, Any]) -> bool:
    payload = {
        key: copy.deepcopy(receipt.get(key))
        for key in (
            "schema", "target_date", "preset", "source_run_uid",
            "source_run_key", "source_generated_at", "decision_at",
            "pit_common_receipt_root_hash", "candidates", "validation_reason",
            "order_authority",
            "automatic_real_order_submission",
        )
    }
    return bool(
        receipt.get("valid") is True
        and payload["schema"] == SCREENER_RECEIPT_SCHEMA
        and payload["order_authority"] is False
        and payload["automatic_real_order_submission"] is False
        and _digest(payload) == receipt.get("receipt_root_hash")
    )


def _load_persisted_screener_runs(
    trade_dates: list[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Read the first server-persisted run for every preset/session.

    Choosing the earliest run by an invariant ordering prevents a backtest from
    selecting a later, better-looking revision.  Missing/legacy/invalid runs
    remain visible and are rejected by the receipt validator.
    """

    if not trade_dates:
        return {}
    frame = read_frame_direct(
        text("""
            SELECT h.id AS history_id, h.run_uid, h.run_key, h.preset,
                   h.requested_date, h.session_date, h.data_date,
                   h.evidence_date, h.observed_at, h.generated_at,
                   h.freshness, h.status, h.source, h.universe,
                   h.result_count, h.request_json, h.selector_json,
                   r.rank_no, r.stock_code AS stored_stock_code,
                   r.payload_json
            FROM st_screener_run_history AS h
            LEFT JOIN st_screener_run_result AS r
              ON r.run_uid=h.run_uid
            WHERE h.session_date BETWEEN :start_date AND :end_date
              AND h.data_date=h.session_date
              AND h.universe='market'
            ORDER BY h.session_date, h.preset,
                     COALESCE(h.observed_at, h.generated_at), h.id,
                     r.rank_no
        """),
        create_batch_engine(),
        params={"start_date": min(trade_dates), "end_date": max(trade_dates)},
    )
    if frame.empty:
        return {}

    def _db_text(value: Any) -> str:
        if value is None:
            return ""
        try:
            if bool(pd.isna(value)):
                return ""
        except (TypeError, ValueError):
            pass
        return str(value)

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    run_meta: dict[str, dict[str, Any]] = {}
    for raw in frame.to_dict(orient="records"):
        uid = _db_text(raw.get("run_uid"))
        run_meta.setdefault(uid, dict(raw))
        payload_text = _db_text(raw.get("payload_json"))
        if payload_text:
            try:
                payload = json.loads(payload_text)
            except (TypeError, ValueError):
                payload = {"__invalid_payload_json__": True}
            groups[uid].append(payload)

    runs_by_identity: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    allowed_days = set(trade_dates)
    allowed_presets = {str(item["key"]) for item in PRESETS}
    for uid, meta in run_meta.items():
        day = _db_text(meta.get("session_date"))[:10]
        preset = _db_text(meta.get("preset"))
        if day not in allowed_days or preset not in allowed_presets:
            continue
        rows = groups.get(uid, [])
        result_count = int(meta.get("result_count") or 0)
        if len(rows) != result_count:
            rows = [{"__result_count_mismatch__": True}]
        try:
            request_payload = json.loads(
                _db_text(meta.get("request_json")) or "{}"
            )
            selector = json.loads(
                _db_text(meta.get("selector_json")) or "{}"
            )
        except (TypeError, ValueError):
            request_payload = {"__invalid_request_json__": True}
            selector = {}
        runs_by_identity[(day, preset)].append({
            "result": {
                "status": _db_text(meta.get("status")),
                "data_date": _db_text(meta.get("data_date"))[:10],
                "evidence_date": (
                    _db_text(meta.get("evidence_date"))[:10] or None
                ),
                "observed_at": _db_text(meta.get("observed_at")) or None,
                "freshness": _db_text(meta.get("freshness")),
                "selector": selector,
                "data": rows,
            },
            "source_run_uid": uid,
            "source_run_key": _db_text(meta.get("run_key")),
            "source_generated_at": _db_text(meta.get("generated_at")),
            "request_payload": request_payload,
            "sort_key": (
                _db_text(meta.get("observed_at"))
                or _db_text(meta.get("generated_at")),
                int(meta.get("history_id") or 0),
                uid,
            ),
        })
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for identity, candidates in runs_by_identity.items():
        selected[identity] = min(candidates, key=lambda item: item["sort_key"])
    return selected


def _collect_signals(
    trade_dates: list[str],
    top: int,
    *,
    persisted_runs: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
) -> tuple[dict[str, list[dict]], list[dict]]:
    signals: dict[str, list[dict]] = defaultdict(list)
    run_audit: list[dict] = []
    try:
        stored = dict(
            persisted_runs
            if persisted_runs is not None
            else _load_persisted_screener_runs(trade_dates)
        )
    except Exception as exc:  # pylint: disable=broad-except
        stored = {}
        load_error = f"{type(exc).__name__}: {str(exc)[:300]}"
    else:
        load_error = ""
    for target_date in trade_dates:
        for preset in PRESETS:
            key = str(preset["key"])
            stored_run = stored.get((target_date, key))
            if not stored_run:
                run_audit.append({
                    "date": target_date,
                    "preset": key,
                    "status": "error",
                    "error": load_error or "immutable_screener_run_receipt_missing",
                    "count": 0,
                })
                continue
            receipt = _freeze_screener_run_receipt(
                target_date=target_date,
                preset=key,
                result=stored_run.get("result") or {},
                source_run_uid=str(stored_run.get("source_run_uid") or ""),
                source_run_key=str(stored_run.get("source_run_key") or ""),
                source_generated_at=stored_run.get("source_generated_at"),
                request_payload=stored_run.get("request_payload") or {},
            )
            accepted = _validate_screener_run_receipt(receipt)
            candidates = receipt.get("candidates") or []
            run_audit.append({
                "date": target_date,
                "preset": key,
                "status": "accepted" if accepted else "excluded",
                "data_date": target_date,
                "freshness": (
                    (stored_run.get("result") or {}).get("freshness") or ""
                ),
                "count": min(len(candidates), top) if accepted else 0,
                "receipt_root_hash": receipt.get("receipt_root_hash"),
                "receipt_valid": accepted,
                "receipt_reason": receipt.get("reason"),
                "decision_at": receipt.get("decision_at"),
                "pit_common_receipt_root_hash": receipt.get(
                    "pit_common_receipt_root_hash"
                ),
            })
            if not accepted:
                continue
            for row in candidates[:top]:
                signals[key].append({
                    "signal_date": target_date,
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    "rank": row["rank"],
                    "score": row["score"],
                    "industry": row["industry"],
                    "preset": key,
                    "screener_receipt_root_hash": receipt[
                        "receipt_root_hash"
                    ],
                    "pit_common_receipt_root_hash": receipt[
                        "pit_common_receipt_root_hash"
                    ],
                    "decision_at": receipt["decision_at"],
                })
    return dict(signals), run_audit


def _screener_run_failures(
    trade_dates: list[str],
    run_audit: list[dict[str, Any]],
) -> dict[str, Any]:
    """Prove every expected strategy/session run was represented and accepted."""

    expected = {
        (target_date, str(preset["key"]))
        for target_date in trade_dates
        for preset in PRESETS
    }
    occurrences: dict[tuple[str, str], int] = defaultdict(int)
    rejected: list[dict[str, Any]] = []
    for row in run_audit:
        identity = (str(row.get("date") or ""), str(row.get("preset") or ""))
        occurrences[identity] += 1
        receipt_root = str(row.get("receipt_root_hash") or "").strip().lower()
        fact_root = str(
            row.get("pit_common_receipt_root_hash") or ""
        ).strip().lower()
        decision_at = str(row.get("decision_at") or "")
        receipt_accepted = bool(
            row.get("receipt_valid") is True
            and _SHA256_RE.fullmatch(receipt_root)
            and _SHA256_RE.fullmatch(fact_root)
            and len(decision_at) > 10
            and decision_at[:10] == identity[0]
        )
        if (
            identity not in expected
            or str(row.get("status") or "") != "accepted"
            or not receipt_accepted
        ):
            rejected.append(dict(row))
    missing = sorted(expected - set(occurrences))
    duplicates = sorted(
        identity for identity, count in occurrences.items() if count != 1
    )
    unexpected = sorted(set(occurrences) - expected)
    canonical_failures = {
        "missing": missing,
        "duplicates": duplicates,
        "unexpected": unexpected,
        "rejected": sorted(
            rejected,
            key=lambda item: (
                str(item.get("date") or ""),
                str(item.get("preset") or ""),
                str(item.get("status") or ""),
            ),
        ),
    }
    return {
        "expected_run_count": len(expected),
        "observed_run_count": len(run_audit),
        "missing_run_count": len(missing),
        "duplicate_identity_count": len(duplicates),
        "unexpected_identity_count": len(unexpected),
        "rejected_run_count": len(rejected),
        "valid": not any(canonical_failures.values()),
        "failure_sha256": hashlib.sha256(
            json.dumps(
                canonical_failures,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "examples": {
            key: value[:100]
            for key, value in canonical_failures.items()
        },
    }


def _forward_return(
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    signal_date: str,
    code: str,
    horizon: int,
) -> tuple[float | None, str]:
    outcome = _forward_execution_outcome(
        price_map,
        trade_dates,
        date_index,
        signal_date,
        code,
        horizon,
        order_value_cny=None,
        base_round_trip_cost=0.0,
    )
    return outcome.get("gross_return"), str(outcome["reason"])


def _trailing_adv_before(
    price_map: Mapping[tuple[str, str], Mapping[str, Any]],
    trade_dates: list[str],
    *,
    code: str,
    target_index: int,
    lookback_sessions: int = DEFAULT_ADV_LOOKBACK_SESSIONS,
    minimum_sessions: int = DEFAULT_MINIMUM_ADV_SESSIONS,
) -> dict[str, Any]:
    """Return average daily turnover known before a target session."""

    amounts: list[float] = []
    source_dates: list[str] = []
    for index in range(max(0, target_index - lookback_sessions), target_index):
        day = trade_dates[index]
        row = price_map.get((code, day))
        try:
            amount = _finite((row or {}).get("amount"), field="historical amount")
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        amounts.append(amount)
        source_dates.append(day)
    if len(amounts) < minimum_sessions:
        return {
            "valid": False,
            "reason": "INSUFFICIENT_PRIOR_ADV_HISTORY",
            "observation_count": len(amounts),
            "latest_source_date": source_dates[-1] if source_dates else "",
        }
    return {
        "valid": True,
        "reason": "PRIOR_SESSION_ADV_VERIFIED",
        "adv_cny": math.fsum(amounts) / len(amounts),
        "observation_count": len(amounts),
        "first_source_date": source_dates[0],
        "latest_source_date": source_dates[-1],
    }


def _forward_execution_outcome(
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    signal_date: str,
    code: str,
    horizon: int,
    *,
    order_value_cny: float | None,
    base_round_trip_cost: float,
    maximum_participation_rate: float = DEFAULT_MAXIMUM_PARTICIPATION_RATE,
    capacity_amount_cny: float | None = None,
    require_immutable_open_receipt: bool | None = None,
) -> dict[str, Any]:
    """Return one fully dispositioned, capacity-aware historical outcome."""

    signal_index = date_index[signal_date]
    entry_index = signal_index + 1
    # A shares bought at T+1 cannot be sold on the same session.  T+N means N
    # complete holding sessions after the T+1 open, not a same-day mark.
    exit_index = entry_index + horizon
    if entry_index >= len(trade_dates) or exit_index >= len(trade_dates):
        return {
            "status": "RIGHT_CENSORED",
            "reason": "insufficient_forward_dates",
            "gross_return": None,
            "net_return": None,
        }
    entry_date = trade_dates[entry_index]
    entry = price_map.get((code, entry_date))
    entry_disposition = daily_bar_execution_disposition(entry, side="BUY")
    if entry_disposition["executable"] is not True:
        return {
            "status": entry_disposition["status"],
            "reason": str(entry_disposition["reason"]).lower(),
            "gross_return": None,
            "net_return": None,
        }
    receipt_required = (
        order_value_cny is not None
        if require_immutable_open_receipt is None
        else bool(require_immutable_open_receipt)
    )
    open_receipt = validate_open_execution_receipt(
        (entry or {}).get("open_execution_receipt"),
        stock_code=code,
        trade_date=entry_date,
        daily_open_price=float(entry_disposition["open_price"]),
    )
    if receipt_required and open_receipt.get("valid") is not True:
        return {
            "status": "HYPOTHETICAL_ONLY",
            "reason": str(
                open_receipt.get("reason") or "missing_immutable_open_receipt"
            ).lower(),
            "gross_return": None,
            "net_return": None,
            "funding_eligible": False,
        }
    entry_open = float(
        open_receipt["execution_price"]
        if open_receipt.get("valid") is True
        else entry_disposition["open_price"]
    )
    entry_close = float(entry.get("close") or 0)
    entry_participation = 0.0
    accepted_order_value = order_value_cny
    entry_adv: dict[str, Any] | None = None
    if order_value_cny is not None:
        desired_quantity = (
            math.floor(order_value_cny / entry_open / 100) * 100
        )
        if desired_quantity <= 0:
            return {
                "status": "KNOWN_UNFILLED",
                "reason": "order_below_one_board_lot",
                "gross_return": None,
                "net_return": None,
                "funding_eligible": False,
            }
        if capacity_amount_cny is None:
            entry_adv = _trailing_adv_before(
                price_map,
                trade_dates,
                code=code,
                target_index=entry_index,
            )
            if entry_adv.get("valid") is not True:
                return {
                    "status": "DATA_BLOCKED",
                    "reason": "insufficient_prior_adv_history",
                    "gross_return": None,
                    "net_return": None,
                    "funding_eligible": False,
                    "entry_adv": entry_adv,
                }
            capacity_turnover = float(entry_adv["adv_cny"])
        else:
            capacity_turnover = _finite(
                capacity_amount_cny,
                field="capacity amount",
            )
        capacity = participation_capped_quantity(
            desired_notional_cny=order_value_cny,
            price=entry_open,
            daily_amount_cny=capacity_turnover,
            maximum_participation_rate=maximum_participation_rate,
        )
        if capacity.get("valid") is not True:
            return {
                "status": "DATA_BLOCKED",
                "reason": "invalid_entry_capacity",
                "gross_return": None,
                "net_return": None,
            }
        if int(capacity.get("quantity") or 0) <= 0:
            return {
                "status": "KNOWN_UNFILLED",
                "reason": "capacity_below_one_board_lot",
                "gross_return": None,
                "net_return": None,
            }
        accepted_quantity = int(capacity.get("quantity") or 0)
        accepted_order_value = float(accepted_quantity * entry_open)
        entry_participation = accepted_order_value / capacity_turnover

    factor = entry_close / entry_open
    for index in range(entry_index + 1, exit_index + 1):
        row = price_map.get((code, trade_dates[index]))
        if not row:
            return {
                "status": "DATA_BLOCKED",
                "reason": "missing_holding_bar",
                "gross_return": None,
                "net_return": None,
            }
        close = float(row.get("close") or 0)
        pre_close = float(row.get("pre_close") or 0)
        if close <= 0 or pre_close <= 0:
            return {
                "status": "DATA_BLOCKED",
                "reason": "missing_official_reference_price",
                "gross_return": None,
                "net_return": None,
            }
        factor *= close / pre_close
    exit_row = price_map.get((code, trade_dates[exit_index]))
    exit_disposition = daily_bar_execution_disposition(exit_row, side="SELL")
    if exit_disposition["executable"] is not True:
        status = (
            "UNRESOLVED_EXIT"
            if exit_disposition["status"] == "KNOWN_UNFILLED"
            else "DATA_BLOCKED"
        )
        return {
            "status": status,
            "reason": str(exit_disposition["reason"]).lower(),
            "gross_return": None,
            "net_return": None,
        }
    value = factor - 1.0
    if not math.isfinite(value):
        return {
            "status": "DATA_BLOCKED",
            "reason": "non_finite_return",
            "gross_return": None,
            "net_return": None,
        }
    exit_participation = 0.0
    exit_adv: dict[str, Any] | None = None
    if accepted_order_value is not None:
        exit_notional = accepted_order_value * factor
        exit_adv = _trailing_adv_before(
            price_map,
            trade_dates,
            code=code,
            target_index=exit_index,
        )
        if exit_adv.get("valid") is not True:
            return {
                "status": "DATA_BLOCKED",
                "reason": "insufficient_prior_exit_adv_history",
                "gross_return": None,
                "net_return": None,
                "funding_eligible": False,
                "exit_adv": exit_adv,
            }
        exit_participation = exit_notional / float(exit_adv["adv_cny"])
        if exit_participation > maximum_participation_rate + 1e-12:
            return {
                "status": "UNRESOLVED_EXIT",
                "reason": "exit_capacity_exceeded",
                "gross_return": None,
                "net_return": None,
            }
    impact = (
        nonlinear_impact_rate(
            participation_rate=entry_participation,
            maximum_participation_rate=maximum_participation_rate,
            base_slippage_rate=DEFAULT_IMPACT_BASE_RATE,
        )
        + nonlinear_impact_rate(
            participation_rate=exit_participation,
            maximum_participation_rate=maximum_participation_rate,
            base_slippage_rate=DEFAULT_IMPACT_BASE_RATE,
        )
    )
    total_cost = float(base_round_trip_cost) + impact
    return {
        "status": "FILLED",
        "reason": "ok",
        "gross_return": value,
        "estimated_cost_rate": total_cost,
        "net_return": value - total_cost,
        "entry_participation_rate": entry_participation,
        "exit_participation_rate": exit_participation,
        "accepted_order_value_cny": accepted_order_value,
        "entry_open_receipt_hash": open_receipt.get("receipt_hash"),
        "entry_open_receipt_source": open_receipt.get("source_provider"),
        "entry_adv": entry_adv,
        "exit_adv": exit_adv,
        "funding_eligible": bool(
            order_value_cny is not None and open_receipt.get("valid") is True
        ),
    }


def _summary(values: list[float], round_trip_cost: float) -> dict[str, Any]:
    if not values:
        return {
            "sample": 0,
            "gross_average_pct": None,
            "gross_win_rate_pct": None,
            "gross_profit_factor": None,
            "net_average_pct": None,
            "net_win_rate_pct": None,
            "net_profit_factor": None,
            "net_average_win_loss": None,
            "net_max_drawdown_pct": None,
        }

    def _metrics(series: list[float]) -> tuple[float, float, float | None, float | None, float]:
        positives = sum(value for value in series if value > 0)
        negatives = -sum(value for value in series if value < 0)
        profit_factor = positives / negatives if negatives > 0 else None
        wins = [value for value in series if value > 0]
        losses = [-value for value in series if value < 0]
        average_win_loss = (
            (sum(wins) / len(wins)) / (sum(losses) / len(losses))
            if wins and losses
            else None
        )
        wealth = 1.0
        peak = 1.0
        max_drawdown = 0.0
        for value in series:
            wealth *= 1.0 + value
            peak = max(peak, wealth)
            if peak > 0:
                max_drawdown = max(max_drawdown, (peak - wealth) / peak)
        return (
            sum(series) / len(series) * 100,
            sum(value > 0 for value in series) / len(series) * 100,
            profit_factor,
            average_win_loss,
            max_drawdown * 100,
        )

    gross_avg, gross_win, gross_pf, _gross_awl, _gross_drawdown = _metrics(values)
    net_values = [value - round_trip_cost for value in values]
    net_avg, net_win, net_pf, net_awl, net_drawdown = _metrics(net_values)
    return {
        "sample": len(values),
        "gross_average_pct": round(gross_avg, 4),
        "gross_win_rate_pct": round(gross_win, 2),
        "gross_profit_factor": round(gross_pf, 4) if gross_pf is not None else None,
        "net_average_pct": round(net_avg, 4),
        "net_win_rate_pct": round(net_win, 2),
        "net_profit_factor": round(net_pf, 4) if net_pf is not None else None,
        "net_average_win_loss": round(net_awl, 4) if net_awl is not None else None,
        "net_max_drawdown_pct": round(net_drawdown, 4),
    }


def _execution_summary(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize only fills while making every non-fill visible to the gate."""

    counts: dict[str, int] = defaultdict(int)
    reasons: dict[str, int] = defaultdict(int)
    gross_values: list[float] = []
    net_values: list[float] = []
    cost_values: list[float] = []
    for outcome in outcomes:
        status = str(outcome.get("status") or "DATA_BLOCKED")
        counts[status] += 1
        reasons[str(outcome.get("reason") or "unknown")] += 1
        if status == "FILLED":
            gross_values.append(float(outcome["gross_return"]))
            net_values.append(float(outcome["net_return"]))
            cost_values.append(float(outcome.get("estimated_cost_rate") or 0.0))
    gross = _summary(gross_values, 0.0)
    net = _summary(net_values, 0.0)
    expected = len(outcomes)
    disposition_count = sum(counts.values())
    evidence_valid = bool(
        expected > 0
        and disposition_count == expected
        and counts.get("DATA_BLOCKED", 0) == 0
        and counts.get("UNRESOLVED_EXIT", 0) == 0
        and counts.get("HYPOTHETICAL_ONLY", 0) == 0
        and all(
            outcome.get("funding_eligible") is True
            for outcome in outcomes
            if str(outcome.get("status") or "") == "FILLED"
        )
    )
    result = {
        "sample": len(net_values),
        "expected_signal_count": expected,
        "execution_disposition_count": disposition_count,
        "execution_disposition_coverage": (
            disposition_count / expected if expected else 0.0
        ),
        "execution_evidence_valid": evidence_valid,
        "execution_status_counts": dict(sorted(counts.items())),
        "execution_reason_counts": dict(sorted(reasons.items())),
        "estimated_average_round_trip_cost_pct": (
            round(sum(cost_values) / len(cost_values) * 100.0, 6)
            if cost_values else None
        ),
        "gross_average_pct": gross["gross_average_pct"],
        "gross_win_rate_pct": gross["gross_win_rate_pct"],
        "gross_profit_factor": gross["gross_profit_factor"],
        "net_average_pct": net["gross_average_pct"],
        "net_win_rate_pct": net["gross_win_rate_pct"],
        "net_profit_factor": net["gross_profit_factor"],
        "net_average_win_loss": (
            _summary(net_values, 0.0)["net_average_win_loss"]
        ),
        "net_max_drawdown_pct": net["net_max_drawdown_pct"],
    }
    if not evidence_valid:
        for field in (
            "net_average_pct", "net_win_rate_pct", "net_profit_factor",
            "net_average_win_loss", "net_max_drawdown_pct",
        ):
            result[field] = None
    return result


def _minimum_effective_sample_size(observation_count: int) -> float:
    if observation_count <= 20:
        return 10.0
    if observation_count <= 60:
        return 30.0
    return 60.0


def _signal_priority(signal: Mapping[str, Any]) -> tuple[Any, ...]:
    preset_order = {
        str(item["key"]): index for index, item in enumerate(PRESETS)
    }
    try:
        rank = int(signal.get("rank") or 10**9)
    except (TypeError, ValueError):
        rank = 10**9
    try:
        score = _finite(signal.get("score"), field="signal score")
    except (TypeError, ValueError):
        score = -math.inf
    return (
        rank,
        -score,
        preset_order.get(str(signal.get("preset") or ""), 10**9),
        str(signal.get("stock_code") or ""),
        str(signal.get("screener_receipt_root_hash") or ""),
    )


def _simulate_shared_account(
    signals: list[dict[str, Any]],
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    horizon: int,
    *,
    evaluation_dates: list[str] | None = None,
    initial_capital_cny: float = DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY,
    base_round_trip_cost: float = 0.002,
    maximum_participation_rate: float = DEFAULT_MAXIMUM_PARTICIPATION_RATE,
    maximum_concurrent_positions: int = DEFAULT_MAXIMUM_CONCURRENT_POSITIONS,
    maximum_stock_weight: float = DEFAULT_MAXIMUM_STOCK_WEIGHT,
    maximum_industry_weight: float = DEFAULT_MAXIMUM_INDUSTRY_WEIGHT,
) -> dict[str, Any]:
    """Replay candidates through one fixed-capital, cash-constrained account."""

    initial_capital = _finite(initial_capital_cny, field="initial capital")
    if initial_capital <= 0:
        raise ValueError("initial capital must be positive")
    if not 1 <= int(maximum_concurrent_positions) <= 10_000:
        raise ValueError("maximum concurrent positions is invalid")
    if not 0 < maximum_stock_weight <= maximum_industry_weight <= 1:
        raise ValueError("portfolio concentration limits are invalid")
    observed_dates = list(evaluation_dates or trade_dates)
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    outcomes: list[dict[str, Any]] = []
    seen_signal_keys: set[tuple[str, str, str]] = set()
    for raw in signals:
        signal = dict(raw)
        signal_day = str(signal.get("signal_date") or "")[:10]
        code = str(signal.get("stock_code") or "").zfill(6)
        preset = str(signal.get("preset") or "")
        identity = (signal_day, code, preset)
        if identity in seen_signal_keys:
            outcomes.append({
                "status": "KNOWN_UNFILLED",
                "reason": "duplicate_signal_identity",
                "gross_return": None,
                "net_return": None,
                "funding_eligible": False,
            })
            continue
        seen_signal_keys.add(identity)
        signal_index = date_index.get(signal_day)
        if signal_index is None or signal_index + 1 >= len(trade_dates):
            outcomes.append({
                "status": "RIGHT_CENSORED",
                "reason": "missing_next_trade_session",
                "gross_return": None,
                "net_return": None,
                "funding_eligible": False,
            })
            continue
        events[trade_dates[signal_index + 1]].append(signal)
    for day in events:
        events[day].sort(key=_signal_priority)

    cash = initial_capital
    positions: list[dict[str, Any]] = []
    nav_records: list[dict[str, Any]] = []
    previous_nav = initial_capital
    cash_breach_count = 0
    maximum_observed_positions = 0
    priority_log: list[dict[str, Any]] = []

    for day in trade_dates:
        day_index = date_index[day]
        # Entries execute at the opening evidence price using only cash and
        # exposures carried from the previous close.  Same-day exits cannot
        # retroactively fund an opening order.
        for signal in events.get(day, []):
            code = str(signal.get("stock_code") or "").zfill(6)
            industry = str(signal.get("industry") or "").strip()
            if not industry:
                outcomes.append({
                    "status": "DATA_BLOCKED",
                    "reason": "missing_pit_industry",
                    "gross_return": None,
                    "net_return": None,
                    "funding_eligible": False,
                })
                continue
            stock_exposure = math.fsum(
                float(
                    position["initial_notional"]
                    if position["entry_index"] == day_index
                    else position["mark_value"]
                )
                for position in positions
                if position["stock_code"] == code
            )
            industry_exposure = math.fsum(
                float(
                    position["initial_notional"]
                    if position["entry_index"] == day_index
                    else position["mark_value"]
                )
                for position in positions
                if position["industry"] == industry
            )
            stock_room = initial_capital * maximum_stock_weight - stock_exposure
            industry_room = (
                initial_capital * maximum_industry_weight - industry_exposure
            )
            desired_notional = min(
                initial_capital * maximum_stock_weight,
                stock_room,
                industry_room,
                cash,
            )
            priority_row = {
                "entry_date": day,
                "stock_code": code,
                "industry": industry,
                "rank": signal.get("rank"),
                "score": signal.get("score"),
                "desired_notional_cny": max(0.0, desired_notional),
            }
            priority_log.append(priority_row)
            if len(positions) >= maximum_concurrent_positions:
                outcomes.append({
                    "status": "KNOWN_UNFILLED",
                    "reason": "maximum_concurrent_positions",
                    "gross_return": None,
                    "net_return": None,
                    "funding_eligible": False,
                })
                continue
            if desired_notional <= 0:
                outcomes.append({
                    "status": "KNOWN_UNFILLED",
                    "reason": "cash_or_concentration_limit",
                    "gross_return": None,
                    "net_return": None,
                    "funding_eligible": False,
                })
                continue
            outcome = _forward_execution_outcome(
                price_map,
                trade_dates,
                date_index,
                str(signal["signal_date"]),
                code,
                horizon,
                order_value_cny=desired_notional,
                base_round_trip_cost=base_round_trip_cost,
                maximum_participation_rate=maximum_participation_rate,
                require_immutable_open_receipt=True,
            )
            outcomes.append(outcome)
            priority_row["disposition"] = outcome.get("status")
            priority_row["accepted_notional_cny"] = float(
                outcome.get("accepted_order_value_cny") or 0.0
            )
            if outcome.get("status") != "FILLED":
                continue
            accepted = float(outcome.get("accepted_order_value_cny") or 0.0)
            if accepted <= 0 or accepted > cash + 1e-6:
                cash_breach_count += 1
                outcomes[-1] = {
                    "status": "DATA_BLOCKED",
                    "reason": "shared_account_cash_invariant_failed",
                    "gross_return": None,
                    "net_return": None,
                    "funding_eligible": False,
                }
                continue
            cash -= accepted
            entry_row = price_map.get((code, day)) or {}
            entry_price = float(
                validate_open_execution_receipt(
                    entry_row.get("open_execution_receipt"),
                    stock_code=code,
                    trade_date=day,
                    daily_open_price=float(entry_row.get("open") or 0),
                )["execution_price"]
            )
            entry_close = float(entry_row.get("close") or 0)
            positions.append({
                "stock_code": code,
                "industry": industry,
                "entry_index": day_index,
                "exit_index": day_index + horizon,
                "initial_notional": accepted,
                "mark_value": accepted * entry_close / entry_price,
                "outcome": outcome,
            })
            maximum_observed_positions = max(
                maximum_observed_positions,
                len(positions),
            )

        next_positions: list[dict[str, Any]] = []
        for position in positions:
            if position["entry_index"] != day_index:
                row = price_map.get((position["stock_code"], day))
                close = float((row or {}).get("close") or 0)
                pre_close = float((row or {}).get("pre_close") or 0)
                if close <= 0 or pre_close <= 0:
                    outcomes.append({
                        "status": "DATA_BLOCKED",
                        "reason": "shared_account_missing_mark",
                        "gross_return": None,
                        "net_return": None,
                        "funding_eligible": False,
                    })
                    position["mark_value"] = 0.0
                else:
                    position["mark_value"] *= close / pre_close
            if day_index >= position["exit_index"]:
                outcome = position["outcome"]
                cash += float(position["initial_notional"]) * (
                    1.0 + float(outcome["net_return"])
                )
            else:
                next_positions.append(position)
        positions = next_positions
        nav = cash + math.fsum(
            float(position["mark_value"]) for position in positions
        )
        if cash < -1e-6 or nav < -1e-6:
            cash_breach_count += 1
        if day in observed_dates:
            daily_return = nav / previous_nav - 1.0 if previous_nav > 0 else -1.0
            nav_records.append({
                "trade_date": day,
                "nav_cny": nav,
                "cash_cny": cash,
                "open_position_count": len(positions),
                "return_pct": daily_return * 100.0,
            })
            previous_nav = nav

    execution = _execution_summary(outcomes)
    statistical_guard = newey_west_nav_statistics(nav_records)
    minimum_ess = _minimum_effective_sample_size(len(nav_records))
    statistical_evidence_valid = bool(
        statistical_guard.get("valid") is True
        and statistical_guard.get("passed") is True
        and float(statistical_guard.get("effective_sample_size") or 0.0)
        >= minimum_ess
    )
    if statistical_guard.get("valid") is True:
        execution.update({
            "net_average_pct": statistical_guard.get("net_expectancy_pct"),
            "net_profit_factor": (
                statistical_guard.get("profit_factor") or {}
            ).get("estimate"),
            "net_average_win_loss": (
                statistical_guard.get("payoff_ratio") or {}
            ).get("estimate"),
        })
    return {
        **execution,
        "initial_capital_cny": initial_capital,
        "ending_nav_cny": nav_records[-1]["nav_cny"] if nav_records else None,
        "daily_nav_records": nav_records,
        "daily_nav_record_count": len(nav_records),
        "cash_constraint_breach_count": cash_breach_count,
        "maximum_observed_concurrent_positions": maximum_observed_positions,
        "portfolio_limits": {
            "maximum_concurrent_positions": maximum_concurrent_positions,
            "maximum_stock_weight": maximum_stock_weight,
            "maximum_industry_weight": maximum_industry_weight,
            "maximum_participation_rate_of_prior_adv": (
                maximum_participation_rate
            ),
        },
        "deterministic_priority": priority_log,
        "statistical_guard": statistical_guard,
        "minimum_effective_sample_size": minimum_ess,
        "statistical_evidence_valid": statistical_evidence_valid,
        "fixed_capital_verified": cash_breach_count == 0,
        "order_authority": False,
        "automatic_real_order_submission": False,
    }


def _statistical_family_gate(
    strategy_results: Mapping[str, Any],
    combined_horizons: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply BY FDR to the complete frozen preset/combined × horizon family."""

    preset_keys = [str(item["key"]) for item in PRESETS]
    trial_inventory = [
        f"{preset_key}|T+{horizon}"
        for preset_key in [*preset_keys, _COMBINED_TRIAL_KEY]
        for horizon in HORIZONS
    ]
    p_values: dict[str, float] = {}
    for preset_key in preset_keys:
        horizons = (strategy_results.get(preset_key) or {}).get("horizons") or {}
        for horizon in HORIZONS:
            trial_key = f"{preset_key}|T+{horizon}"
            guard = (horizons.get(f"T+{horizon}") or {}).get(
                "statistical_guard"
            ) or {}
            p_values[trial_key] = _joint_guard_p_value(guard)
    for horizon in HORIZONS:
        trial_key = f"{_COMBINED_TRIAL_KEY}|T+{horizon}"
        guard = (combined_horizons.get(f"T+{horizon}") or {}).get(
            "statistical_guard"
        ) or {}
        p_values[trial_key] = _joint_guard_p_value(guard)
    family = benjamini_yekutieli_fdr(
        p_values,
        total_hypotheses=len(trial_inventory),
        q=0.05,
        trial_inventory=trial_inventory,
    )
    decisions = {
        str(item.get("key") or ""): dict(item)
        for item in family.get("decisions") or []
    }
    complete = bool(
        family.get("valid") is True
        and set(p_values) == set(trial_inventory)
        and set(decisions) == set(trial_inventory)
    )
    return {
        **family,
        "complete_frozen_family": complete,
        "trial_inventory": trial_inventory,
        "p_values": p_values,
        "decisions_by_key": decisions,
        "order_authority": False,
        "automatic_real_order_submission": False,
    }


def _joint_guard_p_value(guard: Mapping[str, Any]) -> float:
    if guard.get("valid") is not True:
        return 1.0
    try:
        values = (
            _finite(
                guard.get("net_expectancy_one_sided_p_value"),
                field="expectancy p-value",
            ),
            _finite(
                (guard.get("profit_factor") or {}).get(
                    "one_sided_p_value_vs_one"
                ),
                field="profit-factor p-value",
            ),
            _finite(
                (guard.get("payoff_ratio") or {}).get(
                    "one_sided_p_value_vs_one"
                ),
                field="payoff p-value",
            ),
        )
        if any(not 0 <= value <= 1 for value in values):
            return 1.0
        return max(values)
    except (TypeError, ValueError):
        return 1.0


def _release_decision(
    horizons: dict[str, dict[str, Any]],
    audit: dict[str, Any],
    shadow_sessions: int,
    *,
    statistical_family: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate evidence gates without ever granting automatic order authority."""
    expected = len(audit.get("expected_trade_dates") or [])
    actual = len(audit.get("actual_trade_dates") or [])
    coverage = actual / expected if expected else 0.0
    row_count = int(audit.get("row_count") or 0)
    defect_rows = sum(
        int(audit.get(key) or 0)
        for key in (
            "duplicate_business_keys",
            "bad_ohlc",
            "invalid_prices",
            "missing_pre_close_rows",
            "inconsistent_reference_return_rows",
        )
    )
    missing_rate = min(1.0, defect_rows / row_count) if row_count else 1.0
    checks: dict[str, bool] = {
        "universe_coverage": coverage >= RELEASE_THRESHOLDS["minimum_universe_coverage"],
        "data_missing_rate": missing_rate <= RELEASE_THRESHOLDS["maximum_data_missing_rate"],
        "shadow_sessions": shadow_sessions >= RELEASE_THRESHOLDS["minimum_shadow_sessions"],
        "complete_frozen_statistical_family": bool(
            statistical_family
            and statistical_family.get("complete_frozen_family") is True
        ),
    }
    family_decisions = (
        (statistical_family or {}).get("decisions_by_key") or {}
    )
    horizon_details: dict[str, Any] = {}
    for horizon in ("T+1", "T+5", "T+20"):
        metrics = horizons.get(horizon) or {}
        guard = metrics.get("statistical_guard") or {}
        pf_guard = guard.get("profit_factor") or {}
        payoff_guard = guard.get("payoff_ratio") or {}
        trial_key = f"{_COMBINED_TRIAL_KEY}|{horizon}"
        family_decision = family_decisions.get(trial_key) or {}
        detail = {
            "mature_samples": int(metrics.get("sample") or 0)
            >= RELEASE_THRESHOLDS["minimum_mature_samples_per_horizon"],
            "profit_factor": (metrics.get("net_profit_factor") or 0)
            >= RELEASE_THRESHOLDS["minimum_oos_profit_factor"],
            "average_win_loss": (metrics.get("net_average_win_loss") or 0)
            >= RELEASE_THRESHOLDS["minimum_oos_average_win_loss"],
            "execution_evidence": metrics.get("execution_evidence_valid") is True,
            "fixed_capital_nav": bool(
                metrics.get("fixed_capital_verified") is True
                and int(metrics.get("cash_constraint_breach_count") or 0) == 0
                and float(metrics.get("initial_capital_cny") or 0.0) > 0
            ),
            "disposition_coverage": float(
                metrics.get("execution_disposition_coverage") or 0.0
            ) >= RELEASE_THRESHOLDS["minimum_execution_disposition_coverage"],
            "no_data_blocked": int(
                (metrics.get("execution_status_counts") or {}).get(
                    "DATA_BLOCKED", 0
                )
            ) <= RELEASE_THRESHOLDS["maximum_execution_data_blocked"],
            "no_unresolved_exit": int(
                (metrics.get("execution_status_counts") or {}).get(
                    "UNRESOLVED_EXIT", 0
                )
            ) == 0,
            "no_hypothetical_open_fills": int(
                (metrics.get("execution_status_counts") or {}).get(
                    "HYPOTHETICAL_ONLY", 0
                )
            ) <= RELEASE_THRESHOLDS["maximum_hypothetical_only"],
            "hac_guard": bool(
                guard.get("valid") is True
                and guard.get("passed") is True
                and float(
                    guard.get("net_expectancy_one_sided_95_lcb_pct")
                    or -math.inf
                ) > 0
                and float(pf_guard.get("one_sided_95_lcb") or 0.0) > 1
                and float(payoff_guard.get("one_sided_95_lcb") or 0.0) > 1
            ),
            "effective_sample_size": bool(
                float(guard.get("effective_sample_size") or 0.0)
                >= float(metrics.get("minimum_effective_sample_size") or math.inf)
            ),
            "family_by_fdr": family_decision.get("passed") is True,
        }
        horizon_details[horizon] = detail
        checks[f"{horizon}_evidence"] = all(detail.values())
    passed = all(checks.values())
    return {
        "status": "PASS_ADVISORY_RELEASE" if passed else "SHADOW_ONLY",
        "passed": passed,
        "checks": checks,
        "horizons": horizon_details,
        "observed": {
            "universe_coverage": round(coverage, 6),
            "data_missing_rate": round(missing_rate, 6),
            "shadow_sessions": shadow_sessions,
        },
        "thresholds": dict(RELEASE_THRESHOLDS),
        "order_authority": False,
        "automatic_real_order_submission": False,
    }


def _benchmark_comparison(
    pairs: list[tuple[float, float]],
    round_trip_cost: float,
) -> dict[str, Any]:
    if not pairs:
        return {
            "benchmark_sample": 0,
            "market_average_pct": None,
            "gross_excess_average_pct": None,
            "net_excess_average_pct": None,
            "net_excess_win_rate_pct": None,
        }
    market = [benchmark for _value, benchmark in pairs]
    gross_excess = [value - benchmark for value, benchmark in pairs]
    net_excess = [value - benchmark - round_trip_cost for value, benchmark in pairs]
    return {
        "benchmark_sample": len(pairs),
        "market_average_pct": round(sum(market) / len(market) * 100, 4),
        "gross_excess_average_pct": round(
            sum(gross_excess) / len(gross_excess) * 100,
            4,
        ),
        "net_excess_average_pct": round(
            sum(net_excess) / len(net_excess) * 100,
            4,
        ),
        "net_excess_win_rate_pct": round(
            sum(value > 0 for value in net_excess) / len(net_excess) * 100,
            2,
        ),
    }


def _execution_benchmark_comparison(
    pairs: list[tuple[float, float, float]],
) -> dict[str, Any]:
    """Compare gross and fully costed returns with the same-date benchmark."""

    if not pairs:
        return {
            "benchmark_sample": 0,
            "market_average_pct": None,
            "gross_excess_average_pct": None,
            "net_excess_average_pct": None,
            "net_excess_win_rate_pct": None,
        }
    market = [benchmark for _gross, _net, benchmark in pairs]
    gross_excess = [gross - benchmark for gross, _net, benchmark in pairs]
    net_excess = [net - benchmark for _gross, net, benchmark in pairs]
    return {
        "benchmark_sample": len(pairs),
        "market_average_pct": round(sum(market) / len(market) * 100, 4),
        "gross_excess_average_pct": round(
            sum(gross_excess) / len(gross_excess) * 100,
            4,
        ),
        "net_excess_average_pct": round(
            sum(net_excess) / len(net_excess) * 100,
            4,
        ),
        "net_excess_win_rate_pct": round(
            sum(value > 0 for value in net_excess) / len(net_excess) * 100,
            2,
        ),
    }


def _market_benchmark_by_date(
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    stock_codes: list[str],
) -> dict[tuple[str, int], float]:
    benchmark: dict[tuple[str, int], float] = {}
    for signal_date in trade_dates:
        for horizon in HORIZONS:
            values: list[float] = []
            for code in stock_codes:
                value, _reason = _forward_return(
                    price_map,
                    trade_dates,
                    date_index,
                    signal_date,
                    code,
                    horizon,
                )
                if value is not None:
                    values.append(value)
            if values:
                benchmark[(signal_date, horizon)] = sum(values) / len(values)
    return benchmark


def run_backtest(
    start_date: str,
    end_date: str,
    *,
    top: int = 10,
    round_trip_cost: float = 0.002,
) -> dict[str, Any]:
    decision_known_at = datetime.now().replace(microsecond=0)
    dependency_start = (
        datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=80)
    ).strftime("%Y-%m-%d")
    calendar, catalog, daily_truth = _market_truth_window(
        dependency_start,
        end_date,
        decision_known_at=decision_known_at,
    )
    dependency_dates = calendar.sessions_between(dependency_start, end_date)
    trade_dates = [day for day in dependency_dates if start_date <= day <= end_date]
    prices = _load_prices(
        dependency_start,
        end_date,
        catalog_batch_id=catalog.batch_id,
        selected_run_id=daily_truth.run_id,
        run_finished_at=daily_truth.run_finished_at,
    )
    market_truth = daily_truth.as_dict()
    audit = _data_audit(prices, dependency_dates)
    catalog_coverage_failures = _catalog_coverage_failures(
        prices,
        trade_dates=dependency_dates,
        catalog=catalog,
    )
    screener_input_audit = audit_inputs(start_date, end_date)
    hard_failures = {
        "missing_trade_dates": audit["missing_trade_dates"],
        "duplicate_business_keys": audit["duplicate_business_keys"],
        "bad_ohlc": audit["bad_ohlc"],
        "invalid_prices": audit["invalid_prices"],
        "inconsistent_reference_return_rows": audit["inconsistent_reference_return_rows"],
        "screener_inputs": (
            screener_input_audit["hard_failures"]
            if screener_input_audit.get("status") != "pass"
            else {}
        ),
        "catalog_coverage": catalog_coverage_failures,
    }
    if (
        hard_failures["missing_trade_dates"]
        or hard_failures["duplicate_business_keys"]
        or hard_failures["bad_ohlc"]
        or hard_failures["invalid_prices"]
        or hard_failures["inconsistent_reference_return_rows"]
        or hard_failures["screener_inputs"]
        or hard_failures["catalog_coverage"]
    ):
        return {
            "status": "blocked",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "start_date": start_date,
            "end_date": end_date,
            "data_dependency_start": dependency_start,
            "data_audit": audit,
            "screener_input_audit": screener_input_audit,
            "hard_failures": hard_failures,
            "market_truth": market_truth,
        }

    signals, run_audit = _collect_signals(trade_dates, top)
    screener_run_contract = _screener_run_failures(trade_dates, run_audit)
    if screener_run_contract["valid"] is not True:
        hard_failures["screener_runs"] = screener_run_contract
        return {
            "status": "blocked",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "start_date": start_date,
            "end_date": end_date,
            "data_dependency_start": dependency_start,
            "data_audit": audit,
            "screener_input_audit": screener_input_audit,
            "screener_run_audit": run_audit,
            "hard_failures": hard_failures,
            "market_truth": market_truth,
        }
    price_map = {
        (str(row["stock_code"]), str(row["trade_date"])): row
        for row in prices.to_dict(orient="records")
    }
    date_index = {value: index for index, value in enumerate(dependency_dates)}

    strategy_results: dict[str, Any] = {}
    combined_unique: dict[tuple[str, str], dict[str, Any]] = {}
    for preset in PRESETS:
        key = str(preset["key"])
        preset_signals = signals.get(key, [])
        for signal in preset_signals:
            combined_unique.setdefault(
                (signal["signal_date"], signal["stock_code"]),
                signal,
            )
        preset_horizons = {
            f"T+{horizon}": _simulate_shared_account(
                preset_signals,
                price_map,
                dependency_dates,
                date_index,
                horizon,
                evaluation_dates=trade_dates,
                initial_capital_cny=DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY,
                base_round_trip_cost=round_trip_cost,
            )
            for horizon in HORIZONS
        }
        strategy_results[key] = {
            "name": preset["name"],
            "signal_count": len(preset_signals),
            "horizons": preset_horizons,
            "exclusions": {
                f"T+{horizon}:{reason}": count
                for horizon in HORIZONS
                for reason, count in (
                    preset_horizons[f"T+{horizon}"].get(
                        "execution_reason_counts"
                    ) or {}
                ).items()
                if reason != "ok"
            },
        }

    combined_horizons = {
        f"T+{horizon}": _simulate_shared_account(
            list(combined_unique.values()),
            price_map,
            dependency_dates,
            date_index,
            horizon,
            evaluation_dates=trade_dates,
            initial_capital_cny=DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY,
            base_round_trip_cost=round_trip_cost,
        )
        for horizon in HORIZONS
    }
    statistical_family = _statistical_family_gate(
        strategy_results,
        combined_horizons,
    )
    for key, strategy in strategy_results.items():
        for horizon in HORIZONS:
            trial_key = f"{key}|T+{horizon}"
            strategy["horizons"][f"T+{horizon}"]["fdr_decision"] = (
                statistical_family["decisions_by_key"].get(trial_key) or {}
            )
    for horizon in HORIZONS:
        trial_key = f"{_COMBINED_TRIAL_KEY}|T+{horizon}"
        combined_horizons[f"T+{horizon}"]["fdr_decision"] = (
            statistical_family["decisions_by_key"].get(trial_key) or {}
        )
    release_decision = _release_decision(
        combined_horizons,
        audit,
        len(trade_dates),
        statistical_family=statistical_family,
    )

    return {
        "status": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "data_dependency_start": dependency_start,
        "trade_date_count": len(trade_dates),
        "top_per_preset_per_day": top,
        "round_trip_cost": round_trip_cost,
        "market_truth": market_truth,
        "initial_research_capital_cny": DEFAULT_INITIAL_RESEARCH_CAPITAL_CNY,
        "research_order_value_cny": None,
        "maximum_prior_adv_participation_rate": (
            DEFAULT_MAXIMUM_PARTICIPATION_RATE
        ),
        "portfolio_limits": {
            "maximum_concurrent_positions": (
                DEFAULT_MAXIMUM_CONCURRENT_POSITIONS
            ),
            "maximum_stock_weight": DEFAULT_MAXIMUM_STOCK_WEIGHT,
            "maximum_industry_weight": DEFAULT_MAXIMUM_INDUSTRY_WEIGHT,
        },
        "impact_model": {
            "name": "square_root_participation",
            "base_slippage_rate_per_side": DEFAULT_IMPACT_BASE_RATE,
            "board_lot_shares": 100,
        },
        "return_method": (
            "fixed-capital shared NAV; entry T+1 only with an immutable auction/"
            "first-minute receipt; exit after N complete holding sessions; every signal "
            "receives an explicit execution disposition; suspended/one-price-limit/"
            "missing/capacity-constrained sessions never disappear; capacity uses "
            "turnover available before entry (trailing ADV), never same-day amount; returns chain "
            "close/pre_close using the official ex-right reference; net results "
            "include fixed cost and nonlinear participation impact"
        ),
        "data_audit": audit,
        "screener_input_audit": screener_input_audit,
        "screener_run_audit": run_audit,
        "screener_run_contract": screener_run_contract,
        "statistical_family": statistical_family,
        "release_decision": release_decision,
        "strategies": strategy_results,
        "combined": {
            "unique_signal_count": len(combined_unique),
            "horizons": combined_horizons,
            "exclusions": {
                f"T+{horizon}:{reason}": count
                for horizon in HORIZONS
                for reason, count in (
                    combined_horizons[f"T+{horizon}"].get(
                        "execution_reason_counts"
                    ) or {}
                ).items()
                if reason != "ok"
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--round-trip-cost", type=float, default=0.002)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    report = run_backtest(
        args.start_date,
        args.end_date,
        top=max(1, min(args.top, 200)),
        round_trip_cost=max(0.0, args.round_trip_cost),
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload + "\n", encoding="utf-8")
        print(output)
    else:
        print(payload)
    return 0 if report.get("status") == "ok" else 3


if __name__ == "__main__":
    raise SystemExit(main())
