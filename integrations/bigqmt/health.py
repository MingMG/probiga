from __future__ import annotations

"""Layered health checks for the standard-QMT file bridge.

The bridge is healthy only when all three independently produced facts agree:

* the strategy heartbeat is fresh;
* the full-market snapshot file is fresh;
* the consumer has published a fresh receipt for the latest completed
  snapshot ingestion.

Checking the QMT process alone is deliberately insufficient.  The returned
layers also identify whether recovery belongs to the QMT model, the consumer,
or the data-quality gate, so a database problem never authorizes UI clicks.
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
    heartbeat_schema = int(heartbeat.get("schema_version") or 0)
    model_instance_id = str(heartbeat.get("model_instance_id") or "")
    try:
        heartbeat_seq = int(heartbeat.get("heartbeat_seq") or 0)
    except (TypeError, ValueError):
        heartbeat_seq = 0
    model_identity_ok = bool(
        heartbeat_schema < 3 or (model_instance_id and heartbeat_seq > 0)
    )
    try:
        oldest_pending_age = float(
            heartbeat.get("oldest_pending_request_age_seconds")
        )
    except (TypeError, ValueError):
        oldest_pending_age = None
    try:
        oldest_inflight_age = float(
            heartbeat.get("oldest_inflight_request_age_seconds")
        )
    except (TypeError, ValueError):
        oldest_inflight_age = None
    oldest_request_age = max(
        (
            value
            for value in (oldest_pending_age, oldest_inflight_age)
            if value is not None
        ),
        default=None,
    )
    queue_ok = bool(
        oldest_request_age is None or oldest_request_age <= 60.0
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
        "model_instance": model_identity_ok,
        "request_queue": queue_ok,
    }
    failed = [name for name, passed in checks.items() if not passed]
    runtime_checks = {
        key: checks[key]
        for key in ("strategy_heartbeat", "model_instance", "level1_callback")
    }
    transport_checks = {
        key: checks[key]
        for key in ("strategy_heartbeat", "model_instance", "request_queue")
    }
    data_plane_checks = {"full_market_snapshot": checks["full_market_snapshot"]}
    pipeline_checks = {"sync_receipt": checks["sync_receipt"]}
    qmt_owned = any(
        not checks[key]
        for key in (
            "strategy_heartbeat", "model_instance", "request_queue",
            "level1_callback", "full_market_snapshot",
        )
    )
    receipt_quality = str(receipt.get("quality_status") or "").upper()
    consumer_status = str(consumer.get("status") or "").lower()
    consumer_quality_block = bool(
        consumer_status == "data_quality_block"
        or str(consumer.get("quality_status") or "").upper() == "BLOCK"
    )
    if qmt_owned:
        recovery_owner = "QMT_MODEL"
    elif not checks["sync_receipt"] and (
        consumer_quality_block or (receipt_quality and receipt_quality != "PASS")
    ):
        recovery_owner = "DATA_QUALITY"
    elif not checks["sync_receipt"]:
        recovery_owner = "CONSUMER"
    else:
        recovery_owner = "NONE"
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
        "layers": {
            "runtime": {
                "healthy": all(runtime_checks.values()),
                "checks": runtime_checks,
            },
            "transport": {
                "healthy": all(transport_checks.values()),
                "checks": transport_checks,
            },
            "data_plane": {
                "healthy": all(data_plane_checks.values()),
                "checks": data_plane_checks,
            },
            "pipeline": {
                "healthy": all(pipeline_checks.values()),
                "checks": pipeline_checks,
            },
        },
        "recovery_owner": recovery_owner,
        "model_instance_id": model_instance_id or None,
        "heartbeat_seq": heartbeat_seq or None,
        "oldest_pending_request_age_seconds": oldest_pending_age,
        "oldest_inflight_request_age_seconds": oldest_inflight_age,
        "oldest_request_age_seconds": oldest_request_age,
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
