from __future__ import annotations

"""Layered health checks for the standard-QMT file bridge.

The bridge is healthy only when all three independently produced facts agree:

* the strategy heartbeat is fresh;
* the full-market snapshot file is fresh;
* during collection, the consumer has published a fresh receipt for the latest
  completed snapshot ingestion. Outside the calendar-defined collection
  session, a fresh explicit zero-write idle result proves liveness only.

Checking the QMT process alone is deliberately insufficient.  The returned
layers also identify whether recovery belongs to the QMT model, the consumer,
or the data-quality gate, so a database problem never authorizes UI clicks.
"""

import time
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from integrations.bigqmt.spool import bridge_paths, read_json
from integrations.bigqmt.bridge import level1_snapshot, snapshot_protocol_matches


HEALTHY_STRATEGY_STATUSES = {"running", "busy"}


def level1_session_active(now_ts: float) -> bool:
    """Return whether fresh sampled A-share quotes are required right now."""

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
    return result if not isinstance(value, bool) and math.isfinite(result) and result > 0 else None


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
    timestamp = _timestamp(payload.get(timestamp_key))
    return now_ts - timestamp if timestamp is not None else None


def _receipt_source_age(
    receipt: dict[str, Any],
    *,
    now_ts: float,
) -> float | None:
    """Return the age of the snapshot proven by the consumer receipt."""

    source_ts = _timestamp(receipt.get("source_snapshot_token"))
    return now_ts - source_ts if source_ts is not None else None


def evaluate_spool_health(
    qmt_home: Path | str,
    *,
    now_ts: float | None = None,
    heartbeat_max_age_seconds: float = 30.0,
    full_snapshot_max_age_seconds: float = 75.0,
    sync_receipt_max_age_seconds: float = 75.0,
    level1_snapshot_max_age_seconds: float = 15.0,
    require_level1_snapshot: bool | None = None,
    expected_client_pid: int | None = None,
) -> dict[str, Any]:
    """Distinguish runtime liveness from required in-session ingestion."""

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
    level1_required = (
        level1_session_active(current_ts)
        if require_level1_snapshot is None
        else bool(require_level1_snapshot)
    )
    # The consumer evaluates the authoritative trading calendar every cycle.
    # A fresh, explicit zero-write idle result is runtime evidence only: it
    # never attests a database ingestion or renews the previous sync receipt.
    consumer_ts = _timestamp(consumer.get("generated_ts"))
    consumer_fresh = bool(
        consumer_ts is not None
        and 0 <= current_ts - consumer_ts <= float(sync_receipt_max_age_seconds)
    )
    off_session_idle = bool(
        require_level1_snapshot is not True
        and consumer_fresh
        and consumer.get("status") == "idle_market_closed"
        and consumer.get("market_session") == "off_session"
        and consumer.get("freshness_required") is False
        and type(consumer.get("full_rows")) is int
        and consumer["full_rows"] == 0
        and type(consumer.get("tracked_rows")) is int
        and consumer["tracked_rows"] == 0
    )
    if off_session_idle:
        level1_required = False
    live_receipt: dict[str, Any] = {}
    if level1_required:
        _, live_receipt = level1_snapshot(
            qmt_home=qmt_home,
            now=datetime.fromtimestamp(current_ts),
            heartbeat_max_age_seconds=heartbeat_max_age_seconds,
            snapshot_max_age_seconds=level1_snapshot_max_age_seconds,
            event_max_age_seconds=level1_snapshot_max_age_seconds,
            max_ingress_seconds=level1_snapshot_max_age_seconds,
        )
    observed_ts = _timestamp(live_receipt.get("latest_observed_at"))
    observed_age = current_ts - observed_ts if observed_ts is not None else None
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
            and 0 <= receipt_source_age <= float(sync_receipt_max_age_seconds)
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
    if expected_client_pid is not None:
        model_identity_ok = bool(
            model_identity_ok
            and expected_client_pid > 0
            and str(heartbeat.get("pid")) == str(expected_client_pid)
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
            and snapshot_protocol_matches(heartbeat)
            and heartbeat_age is not None
            and 0 <= heartbeat_age <= float(heartbeat_max_age_seconds)
        ),
        "full_market_snapshot": bool(
            current_full_file_token
            and snapshot_protocol_matches(full)
            and full.get("source") == "gj_big_qmt_inner"
            and int(full.get("quote_count") or 0) > 0
            and full_age is not None
            and 0 <= full_age <= float(full_snapshot_max_age_seconds)
        ),
        "sync_receipt": bool(
            str(consumer.get("status") or "").lower()
            not in {"error", "waiting_for_qmt_strategy"}
            and consumer_age is not None
            and 0 <= consumer_age <= float(sync_receipt_max_age_seconds)
            and receipt_attests_fresh_generation
            and str(receipt.get("quality_status") or "").upper() == "PASS"
        ),
        "level1_snapshot": bool(
            not level1_required
            or live_receipt.get("status") == "PASS"
        ),
        "model_instance": model_identity_ok,
        "request_queue": queue_ok,
    }
    required_checks = [
        name for name in checks if name != "sync_receipt" or not off_session_idle
    ]
    failed = [name for name in required_checks if not checks[name]]
    runtime_checks = {
        key: checks[key]
        for key in ("strategy_heartbeat", "model_instance", "level1_snapshot")
    }
    transport_checks = {
        key: checks[key]
        for key in ("strategy_heartbeat", "model_instance", "request_queue")
    }
    data_plane_checks = {"full_market_snapshot": checks["full_market_snapshot"]}
    pipeline_checks = (
        {"consumer_heartbeat": consumer_fresh}
        if off_session_idle
        else {"sync_receipt": checks["sync_receipt"]}
    )
    qmt_owned = any(
        not checks[key]
        for key in (
            "strategy_heartbeat", "model_instance", "request_queue",
            "level1_snapshot", "full_market_snapshot",
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
    elif not off_session_idle and not checks["sync_receipt"] and (
        consumer_quality_block or (receipt_quality and receipt_quality != "PASS")
    ):
        recovery_owner = "DATA_QUALITY"
    elif not off_session_idle and not checks["sync_receipt"]:
        recovery_owner = "CONSUMER"
    else:
        recovery_owner = "NONE"
    return {
        "healthy": not failed,
        "status": (
            "BLOCK" if failed else "IDLE_MARKET_CLOSED" if off_session_idle else "PASS"
        ),
        "reason": (
            ("QMT_MARKET_CLOSED_RUNTIME_HEALTHY" if off_session_idle else "QMT_END_TO_END_HEALTHY")
            if not failed
            else "QMT_END_TO_END_FAILED:" + ",".join(failed)
        ),
        "checks": checks,
        "required_checks": required_checks,
        "sync_receipt_required": not off_session_idle,
        "ingestion_attested": checks["sync_receipt"],
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
        "level1_observed_age_seconds": observed_age,
        "level1_receipt": live_receipt,
        "tracked_snapshot_age_seconds": tracked_age,
        "quote_acquisition_protocol": heartbeat.get("quote_acquisition_protocol"),
        "quote_acquisition_mode": heartbeat.get("quote_acquisition_mode"),
        "receipt_source_age_seconds": receipt_source_age,
        "receipt_matches_current_file": receipt_matches_current,
        "full_file_token": current_full_file_token,
        "receipt_file_token": str(
            receipt.get("source_full_file_token") or ""
        ),
        "receipt": receipt,
    }
