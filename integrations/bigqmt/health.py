from __future__ import annotations

"""End-to-end health checks for the standard-QMT file bridge.

The bridge is healthy only when all three independently produced facts agree:

* the strategy heartbeat is fresh;
* the full-market snapshot file is fresh;
* the consumer has published a fresh receipt for the latest completed
  snapshot ingestion.

Checking the QMT process alone is deliberately insufficient. A process can be
alive while the strategy, snapshot writer, consumer or database publication is
stalled.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Any

from integrations.bigqmt.spool import bridge_paths, read_json


HEALTHY_STRATEGY_STATUSES = {"running", "busy"}


def level1_session_active(now_ts: float) -> bool:
    """Return whether live A-share callbacks are required right now."""

    current = datetime.fromtimestamp(float(now_ts))
    if current.weekday() >= 5:
        return False
    seconds = current.hour * 3600 + current.minute * 60 + current.second
    return (
        9 * 3600 + 30 * 60 <= seconds <= 11 * 3600 + 30 * 60
        or 13 * 3600 <= seconds <= 15 * 3600
    )


def _timestamp(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        try:
            result = datetime.fromisoformat(str(value)).timestamp()
        except (TypeError, ValueError):
            return None
    return result if result > 0 else None


def _latest_callback_ts(
    heartbeat: dict[str, Any],
    tracked: dict[str, Any],
) -> float | None:
    for payload in (heartbeat, tracked):
        value = _timestamp(payload.get("last_callback_ts"))
        if value is not None:
            return value
    quotes = tracked.get("quotes")
    if not isinstance(quotes, dict):
        return None
    values = [
        value
        for tick in quotes.values()
        if isinstance(tick, dict)
        for value in [_timestamp(tick.get("_probiga_received_at"))]
        if value is not None
    ]
    return max(values) if values else None


def file_token(path: Path) -> str:
    if not path.is_file():
        return ""
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _payload_age(
    payload: dict[str, Any],
    path: Path,
    *,
    now_ts: float,
    timestamp_key: str,
) -> float | None:
    raw = payload.get(timestamp_key)
    try:
        timestamp = float(raw)
    except (TypeError, ValueError):
        try:
            timestamp = path.stat().st_mtime
        except OSError:
            return None
    return max(0.0, now_ts - timestamp)


def _receipt_source_age(
    receipt: dict[str, Any],
    *,
    now_ts: float,
) -> float | None:
    """Return the age of the snapshot proven by the consumer receipt."""

    try:
        source_ts = float(receipt.get("source_snapshot_token"))
    except (TypeError, ValueError):
        return None
    return max(0.0, now_ts - source_ts)


def evaluate_spool_health(
    qmt_home: Path | str,
    *,
    now_ts: float | None = None,
    heartbeat_max_age_seconds: float = 30.0,
    full_snapshot_max_age_seconds: float = 75.0,
    sync_receipt_max_age_seconds: float = 75.0,
    level1_callback_max_age_seconds: float = 15.0,
    require_level1_callback: bool | None = None,
) -> dict[str, Any]:
    """Return a fail-closed producer, consumer, and Level-1 health result."""

    current_ts = time.time() if now_ts is None else float(now_ts)
    paths = bridge_paths(qmt_home)
    heartbeat = read_json(paths["heartbeat"])
    full = read_json(paths["full"])
    tracked = read_json(paths["tracked"])
    consumer = read_json(paths["consumer_status"])

    heartbeat_age = _payload_age(
        heartbeat,
        paths["heartbeat"],
        now_ts=current_ts,
        timestamp_key="updated_ts",
    )
    full_age = _payload_age(
        full,
        paths["full"],
        now_ts=current_ts,
        timestamp_key="generated_ts",
    )
    consumer_age = _payload_age(
        consumer,
        paths["consumer_status"],
        now_ts=current_ts,
        timestamp_key="generated_ts",
    )
    tracked_age = _payload_age(
        tracked,
        paths["tracked"],
        now_ts=current_ts,
        timestamp_key="generated_ts",
    )
    callback_ts = _latest_callback_ts(heartbeat, tracked)
    callback_age = (
        max(0.0, current_ts - callback_ts)
        if callback_ts is not None
        else None
    )
    level1_required = (
        level1_session_active(current_ts)
        if require_level1_callback is None
        else bool(require_level1_callback)
    )
    subscription_ok = heartbeat.get("subscription_id") not in {
        None,
        "",
        -1,
        "-1",
    }
    current_full_file_token = file_token(paths["full"])
    receipt = consumer.get("full_sync_receipt")
    if not isinstance(receipt, dict):
        receipt = {}
    receipt_source_age = _receipt_source_age(
        receipt,
        now_ts=current_ts,
    )
    receipt_matches_current = bool(
        current_full_file_token
        and str(receipt.get("source_full_file_token") or "")
        == current_full_file_token
    )
    # A full-market database replacement can take longer than the producer's
    # refresh interval. During that bounded overlap, the file may already be
    # one generation ahead of the latest completed database receipt. Treat
    # that as healthy only while the proven source generation is itself fresh;
    # an arbitrary or old mismatched receipt still fails closed.
    receipt_attests_fresh_generation = bool(
        receipt_matches_current
        or (
            receipt_source_age is not None
            and receipt_source_age <= float(sync_receipt_max_age_seconds)
        )
    )

    checks = {
        "strategy_heartbeat": bool(
            str(heartbeat.get("status") or "").lower()
            in HEALTHY_STRATEGY_STATUSES
            and heartbeat_age is not None
            and heartbeat_age <= float(heartbeat_max_age_seconds)
        ),
        "full_market_snapshot": bool(
            current_full_file_token
            and int(full.get("quote_count") or 0) > 0
            and full_age is not None
            and full_age <= float(full_snapshot_max_age_seconds)
        ),
        "sync_receipt": bool(
            str(consumer.get("status") or "").lower()
            not in {"error", "waiting_for_qmt_strategy"}
            and consumer_age is not None
            and consumer_age <= float(sync_receipt_max_age_seconds)
            and receipt_attests_fresh_generation
            and str(receipt.get("quality_status") or "").upper() == "PASS"
        ),
        "level1_callback": bool(
            not level1_required
            or (
                subscription_ok
                and tracked_age is not None
                and tracked_age <= float(level1_callback_max_age_seconds)
                and callback_age is not None
                and callback_age <= float(level1_callback_max_age_seconds)
            )
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "healthy": not failed,
        "status": "PASS" if not failed else "BLOCK",
        "reason": (
            "QMT_END_TO_END_HEALTHY"
            if not failed
            else "QMT_END_TO_END_FAILED:" + ",".join(failed)
        ),
        "checks": checks,
        "failed_checks": failed,
        "heartbeat_age_seconds": heartbeat_age,
        "full_snapshot_age_seconds": full_age,
        "sync_receipt_age_seconds": consumer_age,
        "level1_required": level1_required,
        "level1_callback_age_seconds": callback_age,
        "tracked_snapshot_age_seconds": tracked_age,
        "subscription_id": heartbeat.get("subscription_id"),
        "receipt_source_age_seconds": receipt_source_age,
        "receipt_matches_current_file": receipt_matches_current,
        "full_file_token": current_full_file_token,
        "receipt_file_token": str(
            receipt.get("source_full_file_token") or ""
        ),
        "receipt": receipt,
    }
