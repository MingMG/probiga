"""Actual read-only capability probes through the production BigQMT model.

The registry's ``gj_qmt`` key identifies the brokerage API catalog. The SDK
metadata records the actual embedded ContextInfo transport; it does not reuse
an external xtquant connection or infer permission from method availability.
"""
from __future__ import annotations

from datetime import date, datetime, time as wall_time
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Callable

from integrations.bigqmt import bridge
from integrations.bigqmt.release_identity import validate_strategy_release_payload
from integrations.qmt.catalog import CORE_PROBE_TO_REGISTRY_KEYS, PROVIDER_ID
from server.common.authoritative_market_clock import authoritative_elapsed_trade_date
from server.common.batch_db import create_batch_engine

ROOT = Path(__file__).resolve().parents[2]
STRATEGY_SOURCE = ROOT / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
IDENTITY_KEYS = (
    "model_instance_id", "strategy_build_sha", "strategy_git_blob",
    "strategy_source_sha256", "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256", "strategy_identity_frozen",
    "strategy_identity_status", "strategy_release_protocol",
    "strategy_identity_protocol",
)
FLOW_FIELDS = (
    "netInflowMostAmount", "netInflowBigAmount",
    "netInflowMediumAmount", "netInflowSmallAmount",
)


class QmtCapabilityTransportUnavailable(RuntimeError):
    """A native request could not reach its serving model."""


def _release(timeout: float) -> dict[str, Any]:
    try:
        result = bridge.capabilities(timeout=timeout)
    except (OSError, TimeoutError) as exc:
        raise QmtCapabilityTransportUnavailable("QMT_CAPABILITY_TRANSPORT_UNAVAILABLE") from exc
    validate_strategy_release_payload(
        result,
        expected_build_sha=str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or ""),
        root=ROOT,
        source_path=STRATEGY_SOURCE,
    )
    if not result.get("model_instance_id"):
        raise RuntimeError("BigQMT capability model instance is absent")
    return result


def probe_capabilities(*, engine: Any, timeout: int = 240, recover_session=None):
    """One recovery budget for the entire read-only set, before any ledger write."""
    deadline = time.monotonic() + max(1, timeout)
    for attempt in range(2):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("capability probe total deadline expired")
        try:
            metadata = capabilities(timeout=min(30, remaining), force=True)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("capability probe total deadline expired")
            samples = core_probe(timeout=remaining, force=True, engine=engine)
            if metadata.get("model_instance_id") != samples.get("model_instance_id"):
                raise RuntimeError("QMT model changed between capability and sample probes")
            return metadata, samples
        except QmtCapabilityTransportUnavailable:
            if attempt:
                raise
            if recover_session is None:
                if os.name != "nt":
                    raise
                from integrations.windows_terminal_recovery import recover_qmt_session_after_failure
                recover_session = recover_qmt_session_after_failure
            if recover_session() is not True:
                raise
            # Discard every prior sample and bind the recovered model anew.
    raise AssertionError("unreachable capability attempt")


def capabilities(*, timeout: int = 30, force: bool = False) -> dict[str, Any]:
    del force  # Each scheduled refresh obtains current frozen model evidence.
    result = _release(float(timeout))
    return {
        "provider": PROVIDER_ID,
        "source": "gj_big_qmt_inner",
        "status": "ok",
        "sdk_module": "BigQMT.ContextInfo",
        "sdk_version": result["strategy_build_sha"],
        "connection_port": None,
        "model_instance_id": result["model_instance_id"],
        "rows": [],  # Existence never becomes an effective capability row.
    }


def _records(value: Any, *, name: str = "", target: str = "") -> list[dict[str, Any]]:
    if name == "announcement":
        from server.common.qmt_announcement_pit import parse_qmt_announcement_frame
        frames = bridge.announcement_frames(value)
        if set(frames) - {"000001.SZ"}:
            raise ValueError("announcement sample returned an unexpected stock")
        target_day = date.fromisoformat(target)
        return [row for symbol, frame in frames.items() for row in parse_qmt_announcement_frame(
            stock_code="000001", qmt_code=symbol, frame=frame,
            window_start=target_day, fact_cutoff_at=datetime.combine(target_day, wall_time(23, 59, 59)),
        )]
    if isinstance(value, dict):
        value = value.get("rows")
    elif hasattr(value, "to_dict"):
        value = value.to_dict("records")
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError("native probe returned an invalid row collection")
    return value


def _validate_capture_identity(capture, before, action):
    if not isinstance(capture, dict):
        raise ValueError("native probe must retain its source response")
    receipts = capture.get("batch_receipts", [capture])
    if not isinstance(receipts, list) or not receipts:
        raise ValueError("native probe response evidence is missing")
    for receipt in receipts:
        if (not isinstance(receipt, dict) or receipt.get("source") != "gj_big_qmt_inner"
            or receipt.get("status") != "ok" or receipt.get("action") != action
            or not receipt.get("request_id")
            or any(before.get(key) != receipt.get(key) for key in IDENTITY_KEYS)):
            raise ValueError("native probe response frozen model identity differs")


def _finite(value: Any, *, positive: bool = False) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and (number > 0 if positive else True)


def _valid_bars(rows: list[dict], symbol: str, target: str) -> bool:
    return bool(rows) and all(
        row.get("qmt_code") == symbol
        and str(row.get("trade_date") or row.get("trade_time") or "")[:10] == target
        and all(_finite(row.get(key), positive=True) for key in ("open", "high", "low", "close"))
        and float(row["low"]) <= min(float(row["open"]), float(row["close"]))
        <= max(float(row["open"]), float(row["close"])) <= float(row["high"])
        and all(_finite(row.get(key)) and float(row[key]) >= 0 for key in ("volume", "amount"))
        for row in rows
    )


def _valid_flow(rows: list[dict], symbol: str, target: str) -> bool:
    return bool(rows) and all(
        row.get("qmt_code") == symbol
        and str(row.get("trade_time") or "")[:10] == target
        and all(_finite(row.get(key)) for key in FLOW_FIELDS)
        for row in rows
    ) and any(float(row[key]) != 0 for row in rows for key in FLOW_FIELDS)


def _probe_plan(target: str) -> dict[str, tuple[str, Callable, Callable]]:
    stock, index = "000001.SZ", "000001.SH"
    result = {}
    for label, symbol in (("stock", stock), ("index", index)):
        result[label + "_daily_bar"] = (
            "kline",
            lambda remaining, symbol=symbol: bridge.kline_capture(
                [symbol], start_date=target, end_date=target,
                dividend_type="none", download_history=True, timeout=remaining,
            ),
            lambda rows, symbol=symbol: _valid_bars(rows, symbol, target),
        )
        result[label + "_minute_bar"] = (
            "minute",
            lambda remaining, symbol=symbol: bridge.minute_capture(
                [symbol], trade_date=target, count=0,
                download_history=True, timeout=remaining,
            ),
            lambda rows, symbol=symbol: _valid_bars(rows, symbol, target),
        )
        result[label + "_instrument"] = (
            "instrument_details",
            lambda remaining, symbol=symbol: bridge._call("instrument_details", stock_codes=[symbol], iscomplete=False, timeout=remaining),
            lambda rows, symbol=symbol: bool(rows) and all(row.get("qmt_code") == symbol for row in rows),
        )
        result[label + "_full_tick"] = (
            "current",
            lambda remaining, symbol=symbol: bridge.current_capture([symbol], timeout=remaining),
            lambda rows, symbol=symbol: bool(rows) and all(
                row.get("qmt_code") == symbol and _finite(row.get("price"), positive=True)
                for row in rows
            ),
        )
    result["sector_list"] = (
        "sector_list", lambda remaining: bridge._call("sector_list", timeout=remaining),
        lambda rows: bool(rows) and all(str(row.get("sector_name") or "").strip() for row in rows),
    )
    for name, sectors in (
        ("stock_universe", ["上证A股", "深证A股", "京市A股"]),
        ("index_universe", ["沪深指数"]),
        ("qmt_sector_indexes", ["迅投一级行业板块加权指数"]),
    ):
        result[name] = (
            "sector_members_many",
            lambda remaining, sectors=sectors: bridge._call("sector_members_many", sector_names=sectors, realtime_tag=-1, timeout=remaining),
            lambda rows: bool(rows) and all(re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", str(row.get("qmt_code") or "")) for row in rows),
        )
    result["trading_calendar"] = (
        "trading_calendar",
        lambda remaining: bridge.trading_calendar_capture("SH", start_date=target, end_date=target, timeout=remaining),
        lambda rows: bool(rows) and any(str(row.get("trade_date") or "")[:10] == target for row in rows),
    )
    result["stock_flow_min"] = (
        "minute_flow_exact",
        lambda remaining: bridge.minute_flow_capture([stock], trade_date=target, timeout=remaining),
        lambda rows: _valid_flow(rows, stock, target),
    )
    result["announcement"] = (
        "announcement",
        lambda remaining: bridge.announcement_capture(
            [stock], start_date=target.replace("-", "") + "000000",
            end_date=target.replace("-", "") + "235959", download_history=True, timeout=remaining,
        ),
        lambda rows: bool(rows) and all(row.get("qmt_code") == stock and row.get("event_date") == target for row in rows),
    )
    return result


def core_probe(*, timeout: int = 240, force: bool = False, engine: Any = None) -> dict[str, Any]:
    del force
    owned_engine = engine is None
    engine = engine or create_batch_engine(future=True)
    try:
        target = authoritative_elapsed_trade_date(engine)
    finally:
        if owned_engine:
            engine.dispose()
    if not target or date.fromisoformat(target).isoformat() != target:
        raise RuntimeError("QMT probe needs an authoritative elapsed trade date")
    deadline = time.monotonic() + max(1, int(timeout))
    before = _release(min(30, max(1, deadline - time.monotonic())))
    actions = set(before.get("actions") or [])
    plan = _probe_plan(target)
    rows = []
    for name in CORE_PROBE_TO_REGISTRY_KEYS:
        row = {"probe_name": name, "status": "UNSUPPORTED_CLIENT", "row_count": 0, "fields": [],
               "error": "The production embedded bridge does not expose this dataset."}
        spec = plan.get(name)
        if spec and spec[0] in actions:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Exhausting the whole probe budget is not a lost login.
                raise TimeoutError("capability probe total deadline expired")
            try:
                capture = spec[1](remaining)
                _validate_capture_identity(capture, before, spec[0])
                records = _records(capture, name=name, target=target)
                row.update(row_count=len(records), fields=sorted({str(key) for item in records for key in item}))
                if not records:
                    row.update(status="NO_DATA", error="The native request returned no sample rows.")
                elif spec[2](records):
                    row.update(status="SUPPORTED", error=None)
                elif name == "stock_flow_min" and all(
                    all(_finite(item.get(key)) and float(item[key]) == 0 for key in FLOW_FIELDS)
                    for item in records
                ):
                    row.update(status="NO_DATA", error="Native L1 zero-filled flow does not prove VIP data permission.")
                else:
                    row.update(status="FAILED", error="Native sample identity, date or fields are invalid.")
            except (OSError, TimeoutError) as exc:
                raise QmtCapabilityTransportUnavailable("QMT_CAPABILITY_TRANSPORT_UNAVAILABLE") from exc
            except Exception as exc:
                reason = getattr(exc, "reason_code", "")
                if name == "announcement" and reason == "QMT_ANNOUNCEMENT_API_UNAVAILABLE":
                    row.update(status="UNSUPPORTED_CLIENT", error=reason)
                else:
                    row.update(status="FAILED", error=type(exc).__name__)
        rows.append(row)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("capability probe total deadline expired")
    after = _release(min(30, remaining))
    if any(before.get(key) != after.get(key) for key in IDENTITY_KEYS):
        raise RuntimeError("BigQMT model identity changed during capability probes")
    failed = any(row["status"] == "FAILED" for row in rows)
    unavailable = any(row["status"] != "SUPPORTED" for row in rows)
    return {
        "status": "error" if failed else "warn" if unavailable else "ok",
        "provider": PROVIDER_ID, "source": "gj_big_qmt_inner",
        "sdk_module": "BigQMT.ContextInfo", "sdk_version": before["strategy_build_sha"],
        "model_instance_id": before["model_instance_id"], "target_date": target,
        "connection_port": None, "rows": rows,
    }
