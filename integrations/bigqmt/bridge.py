from __future__ import annotations

"""File-queue client for the standard QMT built-in Python strategy."""

import os
import re
import time
import hashlib
import json
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
        return max(0.0, now_ts - float(value))
    except (OSError, TypeError, ValueError):
        return None


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
    require_live_callback: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Read only genuine subscription callbacks from the tracked snapshot.

    ``tracked_quotes.json`` is periodically republished even when its cached
    quote book did not change.  A row is therefore Level-1 evidence only when
    the QMT callback attached ``_probiga_received_at`` and the exchange event
    reached that callback within the configured live latency window.  Initial
    ``get_full_tick`` values and historical/backfilled rows fail closed.
    """

    current = (now or datetime.now()).replace(tzinfo=None)
    now_ts = current.timestamp()
    paths = bridge_paths(qmt_home)
    heartbeat = read_json(paths["heartbeat"])
    heartbeat_age = _payload_age_seconds(heartbeat.get("updated_ts"), now_ts)
    heartbeat_status = str(heartbeat.get("status") or "missing").lower()
    heartbeat_ok = bool(
        heartbeat_status in {"running", "busy"}
        and heartbeat_age is not None
        and heartbeat_age <= max(1.0, float(heartbeat_max_age_seconds))
    )
    subscription_ok = heartbeat.get("subscription_id") not in {None, "", -1}

    payload = read_snapshot("tracked", qmt_home=qmt_home, max_age_seconds=None)
    snapshot_age = _payload_age_seconds(payload.get("generated_ts"), now_ts)
    snapshot_ok = bool(
        snapshot_age is not None
        and snapshot_age <= max(1.0, float(snapshot_max_age_seconds))
    )
    quotes = payload.get("quotes")
    callback_symbols = {
        str(symbol).strip().upper()
        for symbol, tick in (quotes.items() if isinstance(quotes, dict) else ())
        if isinstance(tick, dict) and tick.get("_probiga_received_at")
    }

    frame = snapshot_frame(payload)
    if not frame.empty:
        frame = frame.loc[
            frame["qmt_code"].astype(str).str.upper().isin(callback_symbols)
        ].copy()
    wanted = set(_codes(stock_codes))
    if wanted and not frame.empty:
        wanted_bare = {code.split(".", 1)[0].zfill(6) for code in wanted}
        frame = frame.loc[frame["stock_code"].isin(wanted_bare)].copy()

    latest_callback_at: datetime | None = None
    live_frame = frame.iloc[0:0].copy() if not frame.empty else pd.DataFrame()
    if not frame.empty:
        source_at = pd.to_datetime(frame["source_time"], errors="coerce")
        received_at = pd.to_datetime(frame["received_at"], errors="coerce")
        now_value = pd.Timestamp(current)
        ingress_seconds = (received_at - source_at).dt.total_seconds()
        callback_age_seconds = (now_value - received_at).dt.total_seconds()
        same_forward_day = (
            source_at.dt.date == current.date()
        ) & (received_at.dt.date == current.date())
        valid = (
            same_forward_day
            & _session_mask(source_at)
            & ingress_seconds.between(
                -max(0.0, float(future_tolerance_seconds)),
                max(0.0, float(max_ingress_seconds)),
            )
            & callback_age_seconds.between(
                -max(0.0, float(future_tolerance_seconds)),
                max(0.0, float(event_max_age_seconds)),
            )
        )
        live_frame = frame.loc[valid].copy().reset_index(drop=True)
        if received_at.notna().any():
            latest_callback_at = received_at.max().to_pydatetime()

    if not heartbeat_ok:
        reason = "heartbeat_stale_or_unhealthy"
    elif not subscription_ok:
        reason = "subscription_missing"
    elif not snapshot_ok:
        reason = "tracked_snapshot_stale"
    elif require_live_callback and live_frame.empty:
        reason = "no_fresh_live_callback"
    else:
        reason = "live_callback_verified" if require_live_callback else "transport_verified"
    passed = bool(
        heartbeat_ok
        and subscription_ok
        and snapshot_ok
        and (not require_live_callback or not live_frame.empty)
    )
    receipt_payload = {
        "status": "PASS" if passed else "BLOCK",
        "reason": reason,
        "capture_mode": "LIVE_CALLBACK" if passed and require_live_callback else "TRANSPORT_ONLY",
        "heartbeat_status": heartbeat_status,
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_pid": heartbeat.get("pid"),
        "subscription_id": heartbeat.get("subscription_id"),
        "snapshot_age_seconds": snapshot_age,
        "source_batch_id": str(payload.get("batch_id") or ""),
        "source_generated_ts": payload.get("generated_ts"),
        "callback_marked_rows": len(frame),
        "live_rows": len(live_frame),
        "latest_callback_at": (
            latest_callback_at.isoformat(sep=" ", timespec="seconds")
            if latest_callback_at is not None
            else None
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
    live_frame.attrs["level1_receipt"] = dict(receipt_payload)
    return live_frame, receipt_payload


def request_level1_reconnect(
    *,
    qmt_home: Any = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Force the built-in strategy to unsubscribe and subscribe again.

    The QMT strategy watches the immutable watchlist content *and* its mtime.
    Touching the existing file leaves the configured universe unchanged while
    making the next five-second strategy tick execute its resubscribe path.
    """

    watchlist = bridge_paths(qmt_home)["watchlist"]
    if not watchlist.is_file():
        raise FileNotFoundError(f"Big QMT watchlist is missing: {watchlist}")
    previous_mtime_ns = watchlist.stat().st_mtime_ns
    os.utime(watchlist, None)
    return {
        "status": "requested",
        "reason": "tracked_callback_stale",
        "requested_at": (now or datetime.now()).isoformat(
            sep=" ", timespec="seconds"
        ),
        "previous_mtime_ns": previous_mtime_ns,
        "current_mtime_ns": watchlist.stat().st_mtime_ns,
    }


def _call(action: str, *, timeout: int | float | None = None, **params: Any) -> dict[str, Any]:
    return request(action, timeout=_timeout(timeout), **params)


def ping(*, timeout: int | float | None = None) -> dict[str, Any]:
    return _call("ping", timeout=timeout or 20)


def capabilities(*, timeout: int | float | None = None) -> dict[str, Any]:
    return _call("capabilities", timeout=timeout or 20)


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

    return _call(
        "kline",
        timeout=timeout,
        stock_codes=_codes(stock_codes),
        start_date=str(start_date or ""),
        end_date=str(end_date or ""),
        dividend_type=str(dividend_type or "none"),
        download_history=bool(download_history),
        batch_size=batch_size,
    )


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
        # bridge tick between batches.  That gives genuine Level-1 callbacks
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
    response = _call(
        "sector_members_many",
        timeout=timeout or 300,
        sector_names=_codes(sector_names),
        realtime_tag=realtime_tag,
    )
    return pd.DataFrame(response.get("rows") or [])


def instrument_details(
    stock_codes: Iterable[str] | str,
    *,
    iscomplete: bool = False,
    batch_size: int | None = None,
    timeout: int | float | None = None,
) -> pd.DataFrame:
    response = _call(
        "instrument_details",
        timeout=timeout or 300,
        stock_codes=_codes(stock_codes),
        iscomplete=bool(iscomplete),
        batch_size=batch_size,
    )
    return pd.DataFrame(response.get("rows") or [])


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
