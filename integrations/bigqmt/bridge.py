from __future__ import annotations

"""File-queue client for the standard QMT built-in Python strategy."""

import os
import re
import time
import hashlib
import json
import math
import uuid
from collections.abc import Iterable
from datetime import datetime
from typing import Any

import pandas as pd

from integrations.bigqmt.spool import (
    bridge_paths,
    read_json,
    read_snapshot,
    request,
    resolve_big_qmt_home,
    snapshot_frame,
)


LEVEL1_HEARTBEAT_MAX_AGE_SECONDS = 30.0
LEVEL1_SNAPSHOT_MAX_AGE_SECONDS = 15.0
LEVEL1_EVENT_MAX_AGE_SECONDS = 15.0
LEVEL1_MAX_INGRESS_SECONDS = 15.0
LEVEL1_FUTURE_TOLERANCE_SECONDS = 2.0
MINUTE_SPOOL_BATCH_LIMIT = 50
KLINE_SPOOL_BATCH_LIMIT = 20
SECTOR_SPOOL_BATCH_LIMIT = 1
INSTRUMENT_SPOOL_BATCH_LIMIT = 50
CAPABILITIES_CACHE_MAX_AGE_SECONDS = 15.0
MINUTE_FLOW_SPOOL_BATCH_LIMIT = 40
SNAPSHOT_ACQUISITION_PROTOCOL = "probiga.qmt-full-tick-poll.v1"
SNAPSHOT_ACQUISITION_MODE = "full_tick_poll"
SNAPSHOT_ACQUISITION_METHOD = "ContextInfo.get_full_tick"


def _codes(values: Iterable[str] | str) -> list[str]:
    items = [values] if isinstance(values, str) else list(values)
    result: list[str] = []
    seen: set[str] = set()
    for value in items:
        text = str(value or "").strip().upper()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _timeout(value: int | float | None, default: float = 180.0) -> float:
    return max(1.0, float(default if value is None else value))


def _minute_bound(value: str | None, *, end: bool) -> str:
    """Give QMT an explicit intraday bound when a minute count is used.

    QMT accepts a bare trading date for full-day history, but with ``count``
    it interprets the same bare end date as midnight and can return padded
    zero-volume bars.  An explicit day boundary makes the last-N bars end at
    the actual latest market minute.
    """
    text = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return f"{text} {'23:59:59' if end else '00:00:00'}"
    if re.fullmatch(r"\d{8}", text):
        return f"{text}{'235959' if end else '000000'}"
    return text


def is_configured() -> bool:
    home = resolve_big_qmt_home(required=False)
    if home is None:
        return False
    try:
        heartbeat_path = bridge_paths(home)["heartbeat"]
        heartbeat = read_json(heartbeat_path)
        if str(heartbeat.get("status") or "").lower() not in {"running", "starting", "busy"}:
            return False
        age = max(0.0, time.time() - heartbeat_path.stat().st_mtime)
        return age <= float(os.environ.get("BIG_QMT_HEARTBEAT_MAX_AGE_SECONDS", "30"))
    except (OSError, TypeError, ValueError):
        return False


def _payload_age_seconds(value: Any, now_ts: float) -> float | None:
    try:
        timestamp = float(value)
        if isinstance(value, bool) or not math.isfinite(timestamp) or timestamp <= 0:
            return None
        return now_ts - timestamp
    except (OSError, TypeError, ValueError):
        return None


def snapshot_protocol_matches(payload: dict[str, Any]) -> bool:
    return bool(
        payload.get("quote_acquisition_protocol") == SNAPSHOT_ACQUISITION_PROTOCOL
        and payload.get("quote_acquisition_mode") == SNAPSHOT_ACQUISITION_MODE
    )


def is_poll_snapshot(payload: dict[str, Any]) -> bool:
    return snapshot_protocol_matches(payload) and payload.get("source") == "gj_big_qmt_inner"


def _native_observation_times(quotes: Any) -> dict[str, datetime]:
    observed: dict[str, datetime] = {}
    if not isinstance(quotes, dict):
        return observed
    for symbol, tick in quotes.items():
        if (
            not isinstance(tick, dict)
            or tick.get("_probiga_acquisition_method") != SNAPSHOT_ACQUISITION_METHOD
        ):
            continue
        try:
            captured = datetime.fromisoformat(str(tick.get("_probiga_observed_at") or ""))
        except ValueError:
            continue
        if captured.tzinfo is None:
            observed[str(symbol).strip().upper()] = captured
    return observed


def native_snapshot_frame(
    payload: dict[str, Any],
    *,
    short_name_map: dict[str, str] | None = None,
    max_event_age_seconds: float | None = None,
) -> pd.DataFrame:
    """Parse current samples without synthesizing source or observation time."""

    if not payload:
        return pd.DataFrame()
    if payload.get("source") != "gj_big_qmt_inner":
        raise RuntimeError("Full QMT current snapshot source differs")
    if not is_poll_snapshot(payload):
        raise RuntimeError("Full QMT current snapshot acquisition protocol differs")
    now_ts = time.time()
    generated_age = _payload_age_seconds(payload.get("generated_ts"), now_ts)
    if generated_age is None or generated_age < -LEVEL1_FUTURE_TOLERANCE_SECONDS:
        raise RuntimeError("Full QMT current snapshot publication time is invalid")
    observed = _native_observation_times(payload.get("quotes"))
    frame = snapshot_frame(payload, short_name_map=short_name_map, require_native_source_time=True)
    if frame.empty:
        return frame
    captured_at = pd.to_datetime(frame["qmt_code"].map(observed), errors="coerce")
    source_at = pd.to_datetime(frame["source_time"], errors="coerce")
    generated_at = pd.Timestamp(datetime.fromtimestamp(float(payload["generated_ts"])))
    future_limit = pd.Timedelta(seconds=LEVEL1_FUTURE_TOLERANCE_SECONDS)
    valid = (
        captured_at.notna()
        & (source_at <= captured_at + future_limit)
        & (captured_at <= generated_at + future_limit)
        & (captured_at <= pd.Timestamp(datetime.fromtimestamp(now_ts)) + future_limit)
    )
    if max_event_age_seconds is not None:
        max_event_age = float(max_event_age_seconds)
        if not math.isfinite(max_event_age) or max_event_age < 0:
            raise ValueError("Native snapshot event age limit must be finite and nonnegative")
        event_age = (pd.Timestamp(datetime.fromtimestamp(now_ts)) - source_at).dt.total_seconds()
        valid &= event_age.between(-LEVEL1_FUTURE_TOLERANCE_SECONDS, max_event_age)
    frame["received_at"] = captured_at
    return frame.loc[valid].reset_index(drop=True)


def _session_mask(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    seconds = (
        timestamps.dt.hour * 3600
        + timestamps.dt.minute * 60
        + timestamps.dt.second
    )
    morning = seconds.between(9 * 3600 + 30 * 60, 11 * 3600 + 30 * 60)
    afternoon = seconds.between(13 * 3600, 15 * 3600)
    return timestamps.notna() & (timestamps.dt.dayofweek < 5) & (morning | afternoon)


def level1_snapshot(
    stock_codes: Iterable[str] | str = (),
    *,
    qmt_home: Any = None,
    now: datetime | None = None,
    heartbeat_max_age_seconds: float = LEVEL1_HEARTBEAT_MAX_AGE_SECONDS,
    snapshot_max_age_seconds: float = LEVEL1_SNAPSHOT_MAX_AGE_SECONDS,
    event_max_age_seconds: float = LEVEL1_EVENT_MAX_AGE_SECONDS,
    max_ingress_seconds: float = LEVEL1_MAX_INGRESS_SECONDS,
    future_tolerance_seconds: float = LEVEL1_FUTURE_TOLERANCE_SECONDS,
    require_live_snapshot: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate sampled Level-1 quotes from official serialized native reads.

    File publication and API activity never renew a quote's native event or
    first observation time. These samples do not attest a lossless Tick stream.
    """

    current = (now or datetime.now()).replace(tzinfo=None)
    now_ts = current.timestamp()
    paths = bridge_paths(qmt_home)
    heartbeat = read_json(paths["heartbeat"])
    heartbeat_age = _payload_age_seconds(heartbeat.get("updated_ts"), now_ts)
    heartbeat_status = str(heartbeat.get("status") or "missing").lower()
    future_tolerance = max(0.0, float(future_tolerance_seconds))
    heartbeat_ok = bool(
        heartbeat_status in {"running", "busy"}
        and heartbeat_age is not None
        and -future_tolerance <= heartbeat_age <= max(1.0, float(heartbeat_max_age_seconds))
    )

    payload = read_snapshot("tracked", qmt_home=qmt_home, max_age_seconds=None)
    snapshot_age = _payload_age_seconds(payload.get("generated_ts"), now_ts)
    snapshot_ok = bool(
        snapshot_age is not None
        and -future_tolerance <= snapshot_age <= max(1.0, float(snapshot_max_age_seconds))
    )
    protocol_ok = bool(
        snapshot_protocol_matches(heartbeat)
        and is_poll_snapshot(payload)
    )
    observed_times = _native_observation_times(payload.get("quotes"))

    frame = snapshot_frame(payload, require_native_source_time=True)
    if not frame.empty:
        frame = frame.loc[
            frame["qmt_code"].astype(str).str.upper().isin(observed_times)
        ].copy()
    wanted = set(_codes(stock_codes))
    if wanted and not frame.empty:
        wanted_bare = {code.split(".", 1)[0].zfill(6) for code in wanted}
        frame = frame.loc[frame["stock_code"].isin(wanted_bare)].copy()

    latest_observed_at: datetime | None = None
    latest_source_at: datetime | None = None
    live_frame = frame.iloc[0:0].copy() if not frame.empty else pd.DataFrame()
    if not frame.empty:
        source_at = pd.to_datetime(frame["source_time"], errors="coerce")
        # Parse only the genuine per-row native-read marker. The generic
        # display converter may otherwise fall back to file ingestion time.
        received_at = pd.to_datetime(
            frame["qmt_code"].map(observed_times), errors="coerce",
        )
        frame["received_at"] = received_at
        now_value = pd.Timestamp(current)
        ingress_seconds = (received_at - source_at).dt.total_seconds()
        observed_age_seconds = (now_value - received_at).dt.total_seconds()
        event_age_seconds = (now_value - source_at).dt.total_seconds()
        same_forward_day = (
            source_at.dt.date == current.date()
        ) & (received_at.dt.date == current.date())
        valid = (
            same_forward_day
            & _session_mask(source_at)
            & ingress_seconds.between(
                -future_tolerance,
                max(0.0, float(max_ingress_seconds)),
            )
            & observed_age_seconds.between(
                -future_tolerance,
                max(0.0, float(event_max_age_seconds)),
            )
            & event_age_seconds.between(
                -future_tolerance, max(0.0, float(event_max_age_seconds)),
            )
        )
        if snapshot_age is not None:
            valid &= observed_age_seconds >= snapshot_age - future_tolerance
        live_frame = frame.loc[valid].copy().reset_index(drop=True)
        if not live_frame.empty:
            latest_observed_at = pd.to_datetime(live_frame["received_at"]).max().to_pydatetime()
            latest_source_at = pd.to_datetime(live_frame["source_time"]).max().to_pydatetime()

    if not heartbeat_ok:
        reason = "heartbeat_stale_or_unhealthy"
    elif not protocol_ok:
        reason = "acquisition_protocol_mismatch"
    elif not snapshot_ok:
        reason = "tracked_snapshot_stale"
    elif require_live_snapshot and live_frame.empty:
        reason = "no_fresh_live_snapshot"
    else:
        reason = "live_snapshot_verified" if require_live_snapshot else "transport_verified"
    passed = bool(
        heartbeat_ok
        and protocol_ok
        and snapshot_ok
        and (not require_live_snapshot or not live_frame.empty)
    )
    if not passed:
        live_frame = live_frame.iloc[0:0].copy()
    receipt_payload = {
        "status": "PASS" if passed else "BLOCK",
        "reason": reason,
        "capture_mode": "LIVE_SNAPSHOT" if passed and require_live_snapshot else "TRANSPORT_ONLY",
        "quote_acquisition_protocol": SNAPSHOT_ACQUISITION_PROTOCOL,
        "quote_acquisition_mode": SNAPSHOT_ACQUISITION_MODE,
        "snapshot_scope": "SAMPLED_LEVEL1",
        "lossless_tick_stream": False,
        "heartbeat_status": heartbeat_status,
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_pid": heartbeat.get("pid"),
        "snapshot_age_seconds": snapshot_age,
        "source_batch_id": str(payload.get("batch_id") or ""),
        "source_generated_ts": payload.get("generated_ts"),
        "snapshot_marked_rows": len(frame),
        "live_rows": len(live_frame),
        "latest_observed_at": (
            latest_observed_at.isoformat(sep=" ", timespec="seconds")
            if latest_observed_at is not None
            else None
        ),
        "latest_source_at": (
            latest_source_at.isoformat(sep=" ", timespec="seconds")
            if latest_source_at is not None else None
        ),
        "checked_at": current.isoformat(sep=" ", timespec="seconds"),
    }
    receipt_payload["receipt_id"] = hashlib.sha256(
        json.dumps(
            receipt_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:32]
    for key in ("quote_acquisition_protocol", "quote_acquisition_mode", "capture_mode", "lossless_tick_stream"):
        live_frame[key] = receipt_payload[key]
    live_frame.attrs["level1_receipt"] = dict(receipt_payload)
    return live_frame, receipt_payload


def request_level1_refresh(
    *,
    qmt_home: Any = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Request a fresh serialized native read for the configured watchlist.

    The QMT strategy watches the immutable watchlist content *and* its mtime.
    Touching the existing file leaves the configured universe unchanged while
    making the next strategy tick refresh its selected quote universe.
    """

    watchlist = bridge_paths(qmt_home)["watchlist"]
    if not watchlist.is_file():
        raise FileNotFoundError(f"Big QMT watchlist is missing: {watchlist}")
    previous_mtime_ns = watchlist.stat().st_mtime_ns
    os.utime(watchlist, None)
    return {
        "status": "requested",
        "reason": "tracked_snapshot_stale",
        "requested_at": (now or datetime.now()).isoformat(
            sep=" ", timespec="seconds"
        ),
        "previous_mtime_ns": previous_mtime_ns,
        "current_mtime_ns": watchlist.stat().st_mtime_ns,
    }


def _call(
    action: str,
    *,
    timeout: int | float | None = None,
    priority: int | None = None,
    run_id: str | None = None,
    cursor: int = 0,
    **params: Any,
) -> dict[str, Any]:
    return request(
        action,
        timeout=_timeout(timeout),
        priority=priority,
        run_id=run_id,
        cursor=cursor,
        **params,
    )


def ping(*, timeout: int | float | None = None) -> dict[str, Any]:
    return _call("ping", timeout=timeout or 20, priority=0)


def capabilities(*, timeout: int | float | None = None) -> dict[str, Any]:
    paths = bridge_paths()
    try:
        heartbeat = read_json(
            paths["heartbeat"],
            max_age_seconds=CAPABILITIES_CACHE_MAX_AGE_SECONDS,
        )
        cached = read_json(
            paths["capabilities"],
        )
        model_instance_id = str(heartbeat.get("model_instance_id") or "")
        if (
            model_instance_id
            and model_instance_id
            == str(cached.get("model_instance_id") or "")
            and str(heartbeat.get("status") or "").lower()
            in {"running", "busy"}
            and str(cached.get("status") or "").lower() == "ok"
        ):
            return {**cached, "capability_transport": "cached_control_plane"}
    except (OSError, RuntimeError, TypeError, ValueError):
        pass
    return _call("capabilities", timeout=timeout or 20, priority=0)


def current(
    stock_codes: Iterable[str] | str,
    *,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    response = current_capture(
        stock_codes,
        batch_size=batch_size,
        timeout=timeout,
    )
    return pd.DataFrame(response.get("rows") or [])


def current_capture(
    stock_codes: Iterable[str] | str,
    *,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, Any]:
    """Return current rows with the exact loaded-strategy response identity."""

    return _call(
        "current",
        timeout=timeout,
        stock_codes=_codes(stock_codes),
        batch_size=batch_size,
    )


def kline(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    dividend_type: str = "none",
    download_history: bool = True,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    response = kline_capture(
        stock_codes,
        start_date=start_date,
        end_date=end_date,
        dividend_type=dividend_type,
        download_history=download_history,
        batch_size=batch_size,
        timeout=timeout,
    )
    return pd.DataFrame(response.get("rows") or [])


def kline_capture(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    dividend_type: str = "none",
    download_history: bool = True,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, Any]:
    """Return daily bars together with the loaded-strategy release proof.

    Most callers only need a frame and should keep using :func:`kline`.
    Formal publishers use this capture form so the exact request response can
    be bound to QMT's frozen, in-memory strategy identity before any database
    partition is replaced.
    """

    codes = _codes(stock_codes)
    total_timeout = _timeout(timeout, default=180.0)
    deadline = time.monotonic() + total_timeout
    requested_batch_size = int(batch_size or KLINE_SPOOL_BATCH_LIMIT)
    effective_batch_size = max(
        1, min(KLINE_SPOOL_BATCH_LIMIT, requested_batch_size)
    )
    code_batches = [
        codes[offset : offset + effective_batch_size]
        for offset in range(0, len(codes), effective_batch_size)
    ] or [[]]
    run_id = uuid.uuid4().hex
    rows: list[dict[str, Any]] = []
    batch_receipts: list[dict[str, Any]] = []
    envelope: dict[str, Any] = {}
    first_response: dict[str, Any] | None = None
    identity_keys = (
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_frozen",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
    )
    for cursor, code_batch in enumerate(code_batches):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Big QMT kline timed out after {total_timeout:.1f}s total"
            )
        response = _call(
            "kline",
            timeout=remaining,
            priority=90,
            run_id=run_id,
            cursor=cursor * effective_batch_size,
            stock_codes=code_batch,
            start_date=str(start_date or ""),
            end_date=str(end_date or ""),
            dividend_type=str(dividend_type or "none"),
            download_history=bool(download_history),
            batch_size=effective_batch_size,
        )
        batch_rows = response.get("rows") or []
        if not isinstance(batch_rows, list):
            raise RuntimeError("Big QMT kline response rows must be a list")
        if not envelope:
            first_response = response
            envelope = {key: value for key, value in response.items() if key != "rows"}
        elif any(response.get(key) != envelope.get(key) for key in identity_keys):
            raise RuntimeError("Big QMT kline batch strategy identity changed")
        rows.extend(batch_rows)
        batch_receipts.append({
            key: value for key, value in response.items() if key != "rows"
        } | {
            "requested_codes": list(code_batch),
            "row_count": len(batch_rows),
        })
    if len(batch_receipts) == 1 and first_response is not None:
        # Preserve the historical single-request object-identity contract for
        # callers that bind provenance directly to the bridge response.
        first_response["rows"] = rows
        first_response["batch_receipts"] = batch_receipts
        return first_response
    return {**envelope, "rows": rows, "batch_receipts": batch_receipts}


def minute(
    stock_codes: Iterable[str] | str,
    *,
    trade_date: str,
    start_date: str | None = None,
    end_date: str | None = None,
    count: int = 0,
    download_history: bool | None = None,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    capture = minute_capture(
        stock_codes,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        count=count,
        download_history=download_history,
        batch_size=batch_size,
        timeout=timeout,
    )
    return pd.DataFrame(capture.get("rows") or [])


def minute_capture(
    stock_codes: Iterable[str] | str,
    *,
    trade_date: str,
    start_date: str | None = None,
    end_date: str | None = None,
    count: int = 0,
    download_history: bool | None = None,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, Any]:
    """Return all batched minute rows and each response's frozen identity."""

    start_bound = _minute_bound(start_date or trade_date, end=False)
    end_bound = _minute_bound(end_date or trade_date, end=True)
    codes = _codes(stock_codes)
    total_timeout = _timeout(timeout)
    deadline = time.monotonic() + total_timeout
    requested_batch_size = int(batch_size or MINUTE_SPOOL_BATCH_LIMIT)
    effective_batch_size = max(
        1,
        min(MINUTE_SPOOL_BATCH_LIMIT, requested_batch_size),
    )
    code_batches = [
        codes[offset : offset + effective_batch_size]
        for offset in range(0, len(codes), effective_batch_size)
    ] or [[]]

    rows: list[dict[str, Any]] = []
    batch_receipts: list[dict[str, Any]] = []
    for code_batch in code_batches:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Big QMT minute timed out after {total_timeout:.1f}s total"
            )
        # Keep each spool request bounded so the QMT strategy returns to its
        # bridge tick between batches. That gives native Level-1 polling
        # and tracked-snapshot flushing a chance to run during a full-market
        # minute refresh instead of being blocked for the whole universe.
        response = _call(
            "minute",
            timeout=remaining,
            stock_codes=code_batch,
            trade_date=str(trade_date or ""),
            start_date=start_bound,
            end_date=end_bound,
            count=max(0, int(count or 0)),
            download_history=(
                bool(download_history)
                if download_history is not None
                else True
            ),
            batch_size=effective_batch_size,
        )
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Big QMT minute timed out after {total_timeout:.1f}s total"
            )
        batch_rows = response.get("rows") or []
        if not isinstance(batch_rows, list):
            raise RuntimeError("Big QMT minute response rows must be a list")
        rows.extend(batch_rows)
        batch_receipts.append({
            key: value for key, value in response.items() if key != "rows"
        } | {
            "requested_codes": list(code_batch),
            "row_count": len(batch_rows),
        })
    return {
        "status": "ok",
        "source": "gj_big_qmt_inner",
        "bridge_version": "bigqmt_inner_v2",
        "rows": rows,
        "batch_receipts": batch_receipts,
    }


def minute_flow_capture(
    stock_codes: Iterable[str], *, trade_date: str, timeout: int | float = 180,
) -> dict[str, Any]:
    """One exact native feature batch, with the original frozen model proof."""
    supplied = list(stock_codes)
    codes = _codes(supplied)
    if not codes or len(codes) > MINUTE_FLOW_SPOOL_BATCH_LIMIT or len(codes) != len(supplied):
        raise ValueError("Big QMT minute-flow requires 1-40 unique stock codes")
    day = datetime.strptime(str(trade_date), "%Y-%m-%d").date().isoformat()
    if day != trade_date:
        raise ValueError("Big QMT minute-flow trade date is not canonical")
    return _call(
        "minute_flow_exact", timeout=timeout,
        stock_codes=sorted(codes), trade_date=day,
    )


def sector_list(*, timeout: int | float | None = None) -> pd.DataFrame:
    response = _call("sector_list", timeout=timeout or 240)
    return pd.DataFrame(response.get("rows") or [])


def sector_members(
    sector_name: str,
    *,
    realtime_tag: int | str = -1,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    return sector_members_many([sector_name], realtime_tag=realtime_tag, timeout=timeout)


def sector_members_many(
    sector_names: Iterable[str] | str,
    *,
    realtime_tag: int | str = -1,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    names = _codes(sector_names)
    total_timeout = _timeout(timeout, default=300.0)
    deadline = time.monotonic() + total_timeout
    run_id = uuid.uuid4().hex
    rows: list[dict[str, Any]] = []
    for cursor in range(0, len(names), SECTOR_SPOOL_BATCH_LIMIT):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Big QMT sector_members_many timed out after "
                f"{total_timeout:.1f}s total"
            )
        response = _call(
            "sector_members_many",
            timeout=remaining,
            priority=90,
            run_id=run_id,
            cursor=cursor,
            sector_names=names[cursor : cursor + SECTOR_SPOOL_BATCH_LIMIT],
            realtime_tag=realtime_tag,
        )
        batch_rows = response.get("rows") or []
        if not isinstance(batch_rows, list):
            raise RuntimeError("Big QMT sector-members response rows must be a list")
        rows.extend(batch_rows)
    return pd.DataFrame(rows)


def instrument_details(
    stock_codes: Iterable[str] | str,
    *,
    iscomplete: bool = False,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    codes = _codes(stock_codes)
    total_timeout = _timeout(timeout, default=300.0)
    deadline = time.monotonic() + total_timeout
    requested_batch_size = int(batch_size or INSTRUMENT_SPOOL_BATCH_LIMIT)
    effective_batch_size = max(
        1, min(INSTRUMENT_SPOOL_BATCH_LIMIT, requested_batch_size)
    )
    run_id = uuid.uuid4().hex
    rows: list[dict[str, Any]] = []
    for cursor in range(0, len(codes), effective_batch_size):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Big QMT instrument_details timed out after "
                f"{total_timeout:.1f}s total"
            )
        response = _call(
            "instrument_details",
            timeout=remaining,
            priority=90,
            run_id=run_id,
            cursor=cursor,
            stock_codes=codes[cursor : cursor + effective_batch_size],
            iscomplete=bool(iscomplete),
            batch_size=effective_batch_size,
        )
        batch_rows = response.get("rows") or []
        if not isinstance(batch_rows, list):
            raise RuntimeError("Big QMT instrument-details response rows must be a list")
        rows.extend(batch_rows)
    return pd.DataFrame(rows)


def trading_calendar_capture(
    market: str,
    *,
    start_date: str,
    end_date: str,
    source_stock_code: str = "000001.SH",
    timeout: int | float | None = None,
) -> dict[str, Any]:
    """Return rows plus the exact built-in QMT source-method evidence."""

    return _call(
        "trading_calendar",
        timeout=timeout or 300,
        market=str(market or "SH").strip().upper(),
        start_date=str(start_date or ""),
        end_date=str(end_date or ""),
        source_stock_code=str(source_stock_code or "").strip().upper(),
    )


def trading_calendar(
    market: str,
    *,
    start_date: str,
    end_date: str,
    source_stock_code: str = "000001.SH",
    timeout: int | float | None = None,
) -> pd.DataFrame:
    response = trading_calendar_capture(
        market,
        start_date=start_date,
        end_date=end_date,
        source_stock_code=source_stock_code,
        timeout=timeout,
    )
    return pd.DataFrame(response.get("rows") or [])


def announcement_capture(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    download_history: bool = True,
    timeout: int | float | None = None,
) -> dict[str, Any]:
    """Return native announcement frames plus the loaded-strategy identity."""

    return _call(
        "announcement",
        timeout=timeout or 600,
        stock_codes=_codes(stock_codes),
        start_date=str(start_date or ""),
        end_date=str(end_date or ""),
        download_history=bool(download_history),
    )


def announcement_frames(capture: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """Rebuild the exact ``stock -> DataFrame`` shape returned by QMT."""

    raw_frames = capture.get("frames")
    if not isinstance(raw_frames, dict):
        raise RuntimeError("BigQMT announcement response frames are unavailable")
    frames: dict[str, pd.DataFrame] = {}
    for raw_code, payload in raw_frames.items():
        code = str(raw_code or "").strip().upper()
        if not code or code in frames or not isinstance(payload, dict):
            raise RuntimeError("BigQMT announcement response stock map differs")
        raw_rows = payload.get("rows")
        if not isinstance(raw_rows, list):
            raise RuntimeError("BigQMT announcement response rows differ")
        rows: list[dict[str, Any]] = []
        indexes: list[Any] = []
        for item in raw_rows:
            if not isinstance(item, dict) or not isinstance(item.get("row"), dict):
                raise RuntimeError("BigQMT announcement response row differs")
            indexes.append(item.get("index"))
            rows.append(dict(item["row"]))
        frame = pd.DataFrame(rows)
        frame.index = pd.Index(indexes, name=payload.get("index_name"))
        frames[code] = frame
    return frames


def announcement(
    stock_codes: Iterable[str] | str,
    *,
    start_date: str,
    end_date: str,
    download_history: bool = True,
    timeout: int | float | None = None,
) -> dict[str, pd.DataFrame]:
    capture = announcement_capture(
        stock_codes,
        start_date=start_date,
        end_date=end_date,
        download_history=download_history,
        timeout=timeout,
    )
    return announcement_frames(capture)


def index_weight_many(
    index_codes: Iterable[str] | str,
    *,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    response = _call(
        "index_members_many",
        timeout=timeout or 600,
        index_codes=_codes(index_codes),
    )
    return pd.DataFrame(response.get("rows") or [])


def index_weight(index_code: str, *, timeout: int | float | None = None) -> pd.DataFrame:
    return index_weight_many([index_code], timeout=timeout)
