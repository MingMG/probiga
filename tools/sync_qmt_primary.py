from __future__ import annotations

"""Run a canonical data job with signed-in BigQMT as the preferred source.

On the Windows QMT owner, the standard QMT built-in strategy is preferred for
market data. Public sources remain the protected fallback when BigQMT is not
enabled, while the legacy MiniQMT gateway is supported only as an explicit
compatibility option.
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.process_env import build_child_env, child_process_timeout
from tools.env_config import load_project_env


DATASETS: dict[str, tuple[str, dict[str, str]]] = {
    "stock_pool": ("si_all_code", {"SI_ALL_CODE_SOURCE": "qmt", "DATA_SOURCE_CODE_LIST": "qmt"}),
    "index_pool": ("si_all_index_code", {"SI_ALL_INDEX_CODE_SOURCE": "qmt", "DATA_SOURCE_INDEX_LIST": "qmt"}),
    "index_constituent": (
        "si_index_constituent",
        {"SI_INDEX_CONSTITUENT_SOURCE": "qmt", "SI_ALL_INDEX_CODE_SOURCE": "qmt"},
    ),
    "concept_reference": ("si_concept_code_east", {"SI_CONCEPT_SOURCE": "qmt"}),
    "realtime": ("sm_stock_current", {"DATA_SOURCE_CURRENT": "qmt", "SM_STOCK_CURRENT_SOURCE": "qmt"}),
    "daily_kline": ("sm_stock_kline", {"DATA_SOURCE_KLINE": "qmt", "SM_STOCK_KLINE_SOURCE": "qmt"}),
    "minute_price": ("sm_stock_minute", {"DATA_SOURCE_MINUTE": "qmt", "SM_STOCK_MINUTE_SOURCE": "qmt"}),
    "index_current": ("sm_index_current", {"DATA_SOURCE_INDEX_CURRENT": "qmt"}),
    "index_minute": ("sm_index_minute", {"DATA_SOURCE_INDEX_MINUTE": "qmt"}),
    "index_kline": ("sm_index_kline", {"DATA_SOURCE_INDEX_KLINE": "qmt"}),
}


EXTERNAL_FALLBACKS: dict[str, dict[str, str]] = {
    "stock_pool": {"SI_ALL_CODE_SOURCE": "adata", "DATA_SOURCE_CODE_LIST": "adata"},
    "index_pool": {"SI_ALL_INDEX_CODE_SOURCE": "sina", "DATA_SOURCE_INDEX_LIST": "sina"},
    "index_constituent": {
        "SI_INDEX_CONSTITUENT_SOURCE": "sina",
        "SI_ALL_INDEX_CODE_SOURCE": "sina",
        "DATA_SOURCE_INDEX_LIST": "sina",
    },
    "concept_reference": {
        "SI_CONCEPT_SOURCE": "east",
        "DATA_SOURCE_CONCEPT_LIST": "east",
    },
    "realtime": {
        "DATA_SOURCE_CURRENT": "adata",
        "SM_STOCK_CURRENT_SOURCE": "adata",
    },
    "daily_kline": {
        "DATA_SOURCE_KLINE": "adata",
        "SM_STOCK_KLINE_SOURCE": "adata",
    },
    "minute_price": {
        "DATA_SOURCE_MINUTE": "adata",
        "SM_STOCK_MINUTE_SOURCE": "adata",
    },
    "index_current": {
        "DATA_SOURCE_INDEX_CURRENT": "adata",
        "SM_INDEX_CURRENT_SOURCE": "adata",
    },
    "index_minute": {
        "DATA_SOURCE_INDEX_MINUTE": "adata",
        "SM_INDEX_MINUTE_SOURCE": "adata",
    },
    "index_kline": {
        "DATA_SOURCE_INDEX_KLINE": "tencent",
        "SM_INDEX_KLINE_SOURCE": "tencent",
    },
}

BIGQMT_OVERRIDES: dict[str, dict[str, str]] = {
    "concept_reference": {
        "SI_CONCEPT_SOURCE": "bigqmt",
        "SI_INDUSTRY_SOURCE": "bigqmt",
        "DATA_SOURCE_CONCEPT_LIST": "bigqmt",
        "DATA_SOURCE_CODE_LIST": "bigqmt",
    },
    "index_constituent": {
        "SI_INDEX_CONSTITUENT_SOURCE": "bigqmt",
        "SI_ALL_INDEX_CODE_SOURCE": "bigqmt",
        "DATA_SOURCE_INDEX_LIST": "bigqmt",
    },
    "realtime": {
        "DATA_SOURCE_CURRENT": "bigqmt",
        "SM_STOCK_CURRENT_SOURCE": "bigqmt",
    },
    "daily_kline": {
        "DATA_SOURCE_KLINE": "bigqmt",
        "SM_STOCK_KLINE_SOURCE": "bigqmt",
    },
    "minute_price": {
        "DATA_SOURCE_MINUTE": "bigqmt",
        "SM_STOCK_MINUTE_SOURCE": "bigqmt",
    },
    "index_current": {
        "DATA_SOURCE_INDEX_CURRENT": "bigqmt",
        "SM_INDEX_CURRENT_SOURCE": "bigqmt",
    },
    "index_minute": {
        "DATA_SOURCE_INDEX_MINUTE": "bigqmt",
        "SM_INDEX_MINUTE_SOURCE": "bigqmt",
    },
    "index_kline": {
        "DATA_SOURCE_INDEX_KLINE": "bigqmt",
        "SM_INDEX_KLINE_SOURCE": "bigqmt",
    },
}


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _qmt_runtime_available(env: dict[str, str]) -> bool:
    """Probe one quote before starting a full-universe QMT job.

    The gateway process can remain healthy while the Windows QMT terminal is
    sitting at its login dialog.  A real quote request distinguishes that
    state and prevents a scheduler worker from waiting for the full SDK
    timeout before falling through to the external source.
    """
    probe_env = dict(env)
    probe_env.update(
        {
            "QMT_GATEWAY_ATTEMPTS": "1",
            "QMT_GATEWAY_REQUEST_TIMEOUT": "10",
            "QMT_TIMEOUT": "10",
        }
    )
    probe = (
        "import sys; "
        "from integrations.qmt.bridge import current; "
        "frame=current(['000001.SZ','600000.SH'], timeout=10); "
        "sys.exit(0 if frame is not None and not frame.empty else 2)"
    )
    try:
        completed = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=str(ROOT),
            env=probe_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def _bigqmt_runtime_available() -> bool:
    try:
        from integrations.bigqmt import bridge

        if not bridge.is_configured():
            return False
        response = bridge.ping(timeout=20)
        return str(response.get("status") or "").lower() in {
            "ok",
            "ready",
            "running",
        }
    except Exception:
        return False


def _route_external(env: dict[str, str], dataset: str) -> str:
    """Disable QMT for this job and select its validated external fallback."""
    env["QMT_GATEWAY_ENABLED"] = "0"
    env["QMT_GATEWAY_REQUIRED"] = "0"
    env["QMT_TIMEOUT"] = "30"
    env.update(EXTERNAL_FALLBACKS.get(dataset, {}))
    if dataset in {"minute_price", "index_minute"}:
        # The external minute crawler is sequential.  Keep the request rate
        # within the same bounded cadence used by the scheduler's flow job;
        # inheriting the generic 0.5s delay turns a recovery into a 25-minute
        # run for the full universe.
        env.update(
            {
                "MINUTE_REQUEST_DELAY": "0.03",
                "MINUTE_REQUEST_JITTER": "0.02",
                "MINUTE_BATCH_EVERY": "0",
                "MINUTE_FETCH_ATTEMPTS": "2",
                "MINUTE_RETRY_DELAY": "0.3",
            }
        )
    return "external_fallback_qmt_unavailable"


def _level1_collection_window(engine, now: datetime | None = None) -> bool:
    current = now or datetime.now()
    try:
        from server.trading_v2.calendar import is_trade_day

        if not is_trade_day(engine, current.date()):
            return False
    except Exception:
        # Evidence collection fails closed on an unavailable trade calendar.
        return False
    second = current.hour * 3600 + current.minute * 60 + current.second
    return (
        9 * 3600 + 30 * 60 <= second < 11 * 3600 + 31 * 60
        or 13 * 3600 <= second < 15 * 3600 + 60
    )


def _positive_float(
    env: dict[str, str],
    name: str,
    default: float,
    *,
    minimum: float,
    maximum: float,
) -> float:
    try:
        value = float(env.get(name, default))
    except (TypeError, ValueError):
        value = default
    return min(maximum, max(minimum, value))


def _run_bigqmt_level1_window(
    env: dict[str, str],
    *,
    now_fn=datetime.now,
    monotonic_fn=time.monotonic,
    sleep_fn=time.sleep,
) -> dict[str, object]:
    """Continuously consume one scheduler minute of genuine Level-1 events.

    The regular realtime task runs every minute.  Holding each invocation for
    almost that full minute closes the gaps left by one-shot snapshot imports.
    The existing BigQMT consumer still owns current-table publication and its
    end-to-end receipt, while its quote-event writer is wrapped so that only
    callback-attested rows can enter ``st_quote_event_v2``.
    """

    from integrations.bigqmt import bridge
    from integrations.bigqmt.spool import resolve_big_qmt_home
    from server.common.batch_db import create_batch_engine
    from tools import run_big_qmt_bridge as consumer

    duration = _positive_float(
        env,
        "BIG_QMT_LEVEL1_WINDOW_SECONDS",
        50.0,
        minimum=0.0,
        maximum=55.0,
    )
    poll_seconds = _positive_float(
        env,
        "BIG_QMT_LEVEL1_POLL_SECONDS",
        1.0,
        minimum=0.2,
        maximum=5.0,
    )
    reconnect_cooldown = _positive_float(
        env,
        "BIG_QMT_LEVEL1_RECONNECT_COOLDOWN_SECONDS",
        20.0,
        minimum=5.0,
        maximum=60.0,
    )
    engine = create_batch_engine(future=True)
    qmt_home = resolve_big_qmt_home(required=True)
    assert qmt_home is not None
    started_at = now_fn()
    active_session = _level1_collection_window(engine, started_at)
    watchlist = consumer.refresh_watchlist(
        engine,
        qmt_home=qmt_home,
        tracked_limit=int(env.get("BIG_QMT_TRACKED_LIMIT", "280") or 280),
    )
    last_tokens: dict[str, object] = {}
    original_persist = consumer.persist_quote_events
    latest_receipt: dict[str, object] = {}
    last_consumer_result: dict[str, object] = {}
    errors: list[str] = []
    polls = 0
    accepted_rows = 0
    inserted_rows = 0
    reconnects = 0
    last_reconnect_at: float | None = None

    def persist_live_only(target_engine, _rows):
        nonlocal accepted_rows, inserted_rows, latest_receipt
        frame, receipt = bridge.level1_snapshot(
            qmt_home=qmt_home,
            now=now_fn(),
            heartbeat_max_age_seconds=float(
                env.get("BIG_QMT_HEARTBEAT_MAX_AGE_SECONDS", "30")
            ),
            snapshot_max_age_seconds=float(
                env.get("BIG_QMT_LEVEL1_SNAPSHOT_MAX_AGE_SECONDS", "15")
            ),
            event_max_age_seconds=float(
                env.get("BIG_QMT_LEVEL1_EVENT_MAX_AGE_SECONDS", "15")
            ),
            max_ingress_seconds=float(
                env.get("BIG_QMT_LEVEL1_MAX_INGRESS_SECONDS", "15")
            ),
        )
        latest_receipt = receipt
        if receipt.get("status") != "PASS" or frame.empty:
            return {"received": 0, "inserted": 0, "receipt": receipt}
        result = original_persist(
            target_engine,
            frame.to_dict(orient="records"),
        )
        accepted_rows += int(result.get("received") or 0)
        inserted_rows += int(result.get("inserted") or 0)
        return {**result, "receipt": receipt}

    deadline = monotonic_fn() + (duration if active_session else 0.0)
    consumer.persist_quote_events = persist_live_only
    try:
        while True:
            try:
                last_consumer_result = consumer.ingest_once(
                    engine,
                    qmt_home=qmt_home,
                    universe=watchlist["universe"],
                    tracked=watchlist["tracked"],
                    short_name_map=watchlist["short_name_map"],
                    last_tokens=last_tokens,
                )
            except Exception as exc:
                errors.append(str(exc))
            polls += 1

            try:
                _frame, latest_receipt = bridge.level1_snapshot(
                    qmt_home=qmt_home,
                    now=now_fn(),
                    require_live_callback=active_session,
                )
            except Exception as exc:
                latest_receipt = {
                    "status": "BLOCK",
                    "reason": "level1_receipt_unavailable",
                    "error": str(exc),
                }
                errors.append(str(exc))

            current_mono = monotonic_fn()
            reconnectable = latest_receipt.get("reason") in {
                "subscription_missing",
                "no_fresh_live_callback",
                "tracked_snapshot_stale",
            }
            if (
                active_session
                and latest_receipt.get("status") != "PASS"
                and reconnectable
                and (
                    last_reconnect_at is None
                    or current_mono - last_reconnect_at >= reconnect_cooldown
                )
            ):
                try:
                    bridge.request_level1_reconnect(
                        qmt_home=qmt_home,
                        now=now_fn(),
                    )
                    reconnects += 1
                    last_reconnect_at = current_mono
                except Exception as exc:
                    errors.append(f"reconnect failed: {exc}")

            if not active_session or current_mono >= deadline:
                break
            sleep_fn(min(poll_seconds, max(0.0, deadline - current_mono)))
    finally:
        consumer.persist_quote_events = original_persist
        engine.dispose()

    passed = bool(
        not active_session
        or (
            accepted_rows > 0
            and latest_receipt.get("status") == "PASS"
        )
    )
    return {
        "status": "success" if passed else "failed",
        "returncode": 0 if passed else 4,
        "error": "" if passed else "no genuine continuous Level1 callback was captured",
        "capture_mode": "LIVE_FORWARD" if active_session else "OFF_SESSION_SNAPSHOT",
        "active_session": active_session,
        "polls": polls,
        "accepted_rows": accepted_rows,
        "inserted_rows": inserted_rows,
        "reconnects": reconnects,
        "receipt": latest_receipt,
        "consumer": last_consumer_result,
        "transient_errors": errors[-10:],
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": now_fn().isoformat(timespec="seconds"),
    }


def _archive_bigqmt_current_snapshot(
    *,
    now: datetime | None = None,
    minimum_rows: int = 5000,
    minimum_coverage: float = 0.95,
    maximum_age_seconds: int = 120,
    future_tolerance_seconds: int = 10,
) -> int:
    """Archive one receipt-bound BigQMT image for intraday consumers.

    ``sm_stock_current`` is an upserted current-state table, so the newest row
    timestamp alone cannot attest that the entire universe is fresh.  Bind the
    archive to the latest validated BigQMT receipt, require its full coverage,
    and copy only rows ingested after that receipt's source image was produced.
    A MySQL advisory lock plus the immutable source-generation time makes retries
    idempotent without changing the large historical snapshot table schema.
    """

    from sqlalchemy import text

    from server.common.batch_db import create_batch_engine

    engine = create_batch_engine(future=True)
    try:
        with engine.connect() as connection:
            # Serialize both archive attempts and the source-table writer.
            # The bridge uses ``probiga:stock_current`` around full/tracked
            # replacement, so holding the same lock closes the validation to
            # INSERT...SELECT race without blocking ordinary readers.
            lock_specs = (
                ("probiga:qmt-realtime-snapshot-archive", 0),
                ("probiga:stock_current", 5),
            )
            acquired_locks: list[str] = []
            try:
                for lock_name, timeout_seconds in lock_specs:
                    lock_acquired = int(
                        connection.execute(
                            text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
                            {
                                "lock_name": lock_name,
                                "timeout_seconds": timeout_seconds,
                            },
                        ).scalar()
                        or 0
                    )
                    if lock_acquired != 1:
                        raise RuntimeError(
                            f"BigQMT realtime archive lock is busy: {lock_name}"
                        )
                    acquired_locks.append(lock_name)
                connection.commit()
                with connection.begin():
                    db_now = (
                        now.replace(microsecond=0)
                        if now is not None
                        else connection.execute(text("SELECT NOW()"))
                        .scalar_one()
                        .replace(microsecond=0)
                    )
                    receipt = connection.execute(
                        text(
                            """
                            SELECT receipt_id, source_provider,
                                   source_generated_at, heartbeat_at,
                                   expected_count, observed_count, coverage,
                                   published_at, capture_mode, quality_status,
                                   evidence_json
                            FROM st_qmt_realtime_sync_receipt_v2
                            WHERE source_provider = 'gj_big_qmt_inner'
                            ORDER BY published_at DESC, created_at DESC
                            LIMIT 1
                            """
                        )
                    ).mappings().first() or {}
                    if not receipt:
                        raise RuntimeError("BigQMT realtime receipt is unavailable")

                    provider = str(receipt.get("source_provider") or "")
                    capture_mode = str(receipt.get("capture_mode") or "")
                    quality_status = str(receipt.get("quality_status") or "")
                    expected_count = int(receipt.get("expected_count") or 0)
                    observed_count = int(receipt.get("observed_count") or 0)
                    coverage = float(receipt.get("coverage") or 0.0)
                    if (
                        provider != "gj_big_qmt_inner"
                        or capture_mode != "LIVE_FORWARD"
                        or quality_status != "PASS"
                    ):
                        raise RuntimeError("BigQMT realtime receipt is not PASS")
                    coverage_floor = max(0.95, float(minimum_coverage))
                    row_floor = max(5000, int(minimum_rows))
                    if (
                        expected_count < row_floor
                        or observed_count < row_floor
                        or observed_count > expected_count
                        or coverage < coverage_floor
                        or abs(coverage - (observed_count / expected_count)) > 0.01
                    ):
                        raise RuntimeError(
                            "BigQMT realtime receipt coverage is below contract"
                        )

                    timestamps: dict[str, datetime] = {}
                    for field in (
                        "source_generated_at",
                        "heartbeat_at",
                        "published_at",
                    ):
                        value = receipt.get(field)
                        if not isinstance(value, datetime):
                            raise RuntimeError(
                                f"BigQMT realtime receipt {field} is unavailable"
                            )
                        timestamps[field] = value.replace(microsecond=0)
                    future_limit = db_now + timedelta(
                        seconds=max(0, int(future_tolerance_seconds))
                    )
                    for field, value in timestamps.items():
                        if value > future_limit:
                            raise RuntimeError(
                                f"BigQMT realtime receipt {field} is in the future"
                            )
                        if (db_now - value).total_seconds() > max(
                            1, int(maximum_age_seconds)
                        ):
                            raise RuntimeError(
                                f"BigQMT realtime receipt {field} is stale"
                            )

                    try:
                        evidence = json.loads(str(receipt.get("evidence_json") or ""))
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "BigQMT realtime receipt evidence is invalid"
                        ) from exc
                    full_batch_id = str(evidence.get("full_batch_id") or "")
                    full_quote_count = int(evidence.get("full_quote_count") or 0)
                    if not full_batch_id or full_quote_count != observed_count:
                        raise RuntimeError(
                            "BigQMT realtime receipt evidence does not match coverage"
                        )

                    source_params = {
                        "provider": provider,
                        "quality_status": "VALIDATED",
                        "source_fresh_after": db_now
                        - timedelta(seconds=max(1, int(maximum_age_seconds))),
                        "future_limit": future_limit,
                        "full_batch_id": full_batch_id,
                    }
                    source = connection.execute(
                        text(
                            """
                            SELECT COUNT(*) AS row_count,
                                   COUNT(DISTINCT stock_code) AS stock_count,
                                   SUM(batch_id = :full_batch_id) AS full_batch_rows,
                                   SUM(
                                       snapshot_at >= :source_fresh_after
                                       AND snapshot_at <= :future_limit
                                   ) AS fresh_source_rows,
                                   SUM(
                                       received_at >= :source_fresh_after
                                       AND received_at <= :future_limit
                                   ) AS fresh_ingest_rows,
                                   MIN(received_at) AS first_received_at,
                                   MAX(received_at) AS latest_received_at
                            FROM sm_stock_current
                            WHERE data_source = :provider
                              AND quality_status = :quality_status
                              AND received_at <= :future_limit
                            """
                        ),
                        source_params,
                    ).mappings().first() or {}
                    row_count = int(source.get("row_count") or 0)
                    stock_count = int(source.get("stock_count") or 0)
                    full_batch_rows = int(source.get("full_batch_rows") or 0)
                    fresh_source_rows = int(source.get("fresh_source_rows") or 0)
                    fresh_ingest_rows = int(source.get("fresh_ingest_rows") or 0)
                    required_full_rows = math.ceil(observed_count * coverage_floor)
                    if (
                        row_count != observed_count
                        or stock_count != observed_count
                        or full_batch_rows < required_full_rows
                        or fresh_source_rows < max(row_floor, required_full_rows)
                        or fresh_ingest_rows < max(row_floor, required_full_rows)
                    ):
                        raise RuntimeError(
                            "BigQMT current rows do not match the validated receipt"
                        )

                    # ``published_at`` is refreshed in place when the bridge
                    # republishes the same source token. ``source_generated_at``
                    # is part of that token's immutable evidence and therefore
                    # provides a stable retry key.
                    snapshot_at = timestamps["source_generated_at"]
                    existing = connection.execute(
                        text(
                            """
                            SELECT COUNT(*) AS row_count,
                                   COUNT(DISTINCT stock_code) AS stock_count
                            FROM sm_rt_quote_snapshot
                            WHERE snapshot_at = :snapshot_at
                            """
                        ),
                        {"snapshot_at": snapshot_at},
                    ).mappings().first() or {}
                    existing_rows = int(existing.get("row_count") or 0)
                    existing_stocks = int(existing.get("stock_count") or 0)
                    if existing_rows or existing_stocks:
                        if (
                            existing_rows == observed_count
                            and existing_stocks == observed_count
                        ):
                            return existing_rows
                        raise RuntimeError(
                            "BigQMT realtime archive key already contains a partial batch"
                        )

                    inserted = connection.execute(
                        text(
                            """
                            INSERT INTO sm_rt_quote_snapshot (
                                stock_code,
                                short_name,
                                price,
                                `change`,
                                change_pct,
                                volume,
                                amount,
                                snapshot_at
                            )
                            SELECT
                                stock_code,
                                short_name,
                                price,
                                `change`,
                                change_pct,
                                volume,
                                amount,
                                :snapshot_at
                            FROM sm_stock_current
                            WHERE data_source = :provider
                              AND quality_status = :quality_status
                              AND received_at <= :future_limit
                            """
                        ),
                        {**source_params, "snapshot_at": snapshot_at},
                    ).rowcount
                    inserted_rows = int(inserted or 0)
                    if inserted_rows != observed_count:
                        raise RuntimeError(
                            "BigQMT realtime archive row count changed during write"
                        )
                    return inserted_rows
            finally:
                try:
                    if connection.in_transaction():
                        connection.rollback()
                    for lock_name in reversed(acquired_locks):
                        connection.execute(
                            text("SELECT RELEASE_LOCK(:lock_name)"),
                            {"lock_name": lock_name},
                        )
                    connection.commit()
                except Exception:
                    connection.invalidate()
    finally:
        engine.dispose()


def run_dataset(
    dataset: str,
    *,
    date_str: str = "",
    minute_count: int = 0,
    start_date: str = "",
    end_date: str = "",
) -> dict[str, object]:
    table_name, overrides = DATASETS[dataset]
    env = build_child_env(ROOT)
    env.update(overrides)
    # A QMT-primary scheduler job is always a canonical full-universe job.
    # Production may carry conservative SM_MAX_* values for ad-hoc/manual
    # crawlers; those limits must not silently turn acceptance into sampling.
    env["SM_MAX_STOCKS"] = "0"
    env["SM_MAX_INDEXES"] = "0"
    env["SM_MAX_CONCEPTS"] = "0"
    # This entrypoint is specifically the production QMT-primary route.  Force
    # the loopback gateway so Linux never tries to spawn a Windows QMT worker.
    env["QMT_GATEWAY_ENABLED"] = "1"
    env["QMT_GATEWAY_REQUIRED"] = "1"
    # A hung Windows SDK call must fail over once to the protected external
    # quote source; repeated gateway retries otherwise block the minute poll.
    env["QMT_GATEWAY_ATTEMPTS"] = "1"
    env.setdefault("QMT_GATEWAY_URL", "http://127.0.0.1:18765")
    env["QMT_GATEWAY_REQUEST_TIMEOUT"] = "90"
    env["QMT_TIMEOUT"] = "90"
    env.setdefault("CURRENT_MIN_COVERAGE", "0.98")
    env.setdefault("QMT_CURRENT_MIN_COVERAGE", "0.98")
    env.setdefault("QMT_CURRENT_MAX_AGE_SECONDS", "180")
    env.setdefault("QMT_KLINE_MIN_COVERAGE", "0.90")
    env.setdefault("QMT_MINUTE_MIN_COVERAGE", "0.85")
    env.setdefault("QMT_PRODUCTION_KLINE_BATCH_SIZE", "200")
    # Twenty bars for 200 symbols stay comfortably bounded while reducing
    # full-market spool round trips from roughly 140 to 28.  The previous
    # 40-symbol setting repeatedly stopped around 4,000 stocks before the next
    # intraday decision tick could use the data.
    env.setdefault("QMT_PRODUCTION_MINUTE_BATCH_SIZE", "200")
    env.setdefault("BIG_QMT_MINUTE_BATCH_SIZE", "200")
    env.setdefault("QMT_PRODUCTION_INDEX_KLINE_BATCH_SIZE", "40")
    env.setdefault("QMT_PRODUCTION_INDEX_MINUTE_BATCH_SIZE", "40")
    env.setdefault("QMT_MINUTE_DB_CHUNK_SIZE", "1000")
    env.setdefault("QMT_INDEX_DB_CHUNK_SIZE", "1000")
    bigqmt_enabled = _enabled(env.get("BIG_QMT_BRIDGE_ENABLED"))
    bigqmt_available = (
        dataset in BIGQMT_OVERRIDES
        and bigqmt_enabled
        and _bigqmt_runtime_available()
    )
    legacy_miniqmt_enabled = _enabled(env.get("LEGACY_MINIQMT_ENABLED"))
    source_policy = "bigqmt_primary" if bigqmt_available else "qmt_primary_external_fallback"
    if bigqmt_available:
        env.update(BIGQMT_OVERRIDES[dataset])
        # Standard BigQMT communicates through its file spool, not the
        # legacy HTTP MiniQMT gateway.
        env["QMT_GATEWAY_ENABLED"] = "0"
        env["QMT_GATEWAY_REQUIRED"] = "0"
        # Once the authenticated BigQMT route has been selected, a failed QMT
        # request must remain a visible failure.  Silently switching the same
        # run to a public source would make its provenance claim untrue.
        env["QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK"] = "0"
    elif not legacy_miniqmt_enabled:
        source_policy = _route_external(env, dataset)
        source_policy = "external_primary_miniqmt_disabled"
    elif not _qmt_runtime_available(env):
        source_policy = _route_external(env, dataset)
        print(
            f"QMT quote preflight failed; routing {dataset} to the external fallback.",
            flush=True,
        )
    if dataset in {"minute_price", "index_minute"}:
        env["QMT_MINUTE_COUNT"] = str(max(0, int(minute_count)))
        env["QMT_INDEX_MINUTE_COUNT"] = str(max(0, int(minute_count)))
        if date_str.strip():
            env["MYQUANT_MINUTE_DATE"] = date_str.strip()
    if dataset == "minute_price" and not date_str.strip():
        # The recurring intraday stock-minute task must not recrawl the full
        # market during the midday break or outside the trading session.
        env["MINUTE_SKIP_CLOSED"] = "1"

    if dataset == "realtime" and source_policy != "qmt_primary_external_fallback" and source_policy != "bigqmt_primary":
        command = [
            sys.executable,
            "scripts/sync_realtime_quotes.py",
            "--min-coverage",
            "0.70",
            "--no-skip-closed",
            "--json",
        ]
    elif dataset == "concept_reference":
        command = [sys.executable, "tools/sync_qmt_concept_reference.py"]
    elif dataset == "index_kline" and start_date.strip():
        command = [
            sys.executable,
            "-m",
            "biz.stock_market.sync_stock_market",
            "--only",
            "index_kline",
            "--limit",
            "-1",
            "--kline-start",
            start_date.strip(),
            "--kline-end",
            end_date.strip() or datetime.now().strftime("%Y-%m-%d"),
        ]
    else:
        command = [sys.executable, "tools/run_single_table.py", table_name]
    if date_str:
        command.append(date_str)
    started = datetime.now()
    level1_capture: dict[str, object] | None = None
    if dataset == "realtime" and source_policy == "bigqmt_primary":
        try:
            level1_capture = dict(_run_bigqmt_level1_window(env))
            returncode = int(level1_capture.get("returncode") or 0)
            error = str(level1_capture.get("error") or "")
            if returncode == 0:
                if (
                    level1_capture.get("active_session") is True
                    and level1_capture.get("capture_mode") == "LIVE_FORWARD"
                ):
                    level1_capture["snapshot_rows"] = (
                        _archive_bigqmt_current_snapshot()
                    )
                    level1_capture["snapshot_archive_status"] = "archived"
                else:
                    # The off-session refresh is allowed to keep current data
                    # available, but it must never mint a fresh intraday
                    # historical snapshot from a transport-only receipt.
                    level1_capture["snapshot_archive_status"] = (
                        "skipped_off_session"
                    )
        except Exception as exc:
            returncode = 4
            error = f"BigQMT Level1 continuous capture failed: {exc}"
            level1_capture = {
                "status": "failed",
                "returncode": returncode,
                "error": error,
            }
    else:
        timeout_seconds = child_process_timeout(2 * 60 * 60, env_name="QMT_PRIMARY_JOB_TIMEOUT")
        try:
            completed = subprocess.run(
                command,
                cwd=str(ROOT),
                env=env,
                timeout=timeout_seconds,
                check=False,
            )
            returncode = int(completed.returncode)
            error = ""
        except subprocess.TimeoutExpired:
            returncode = 124
            error = f"job timed out after {timeout_seconds}s"
    attestation: dict[str, object] | None = None
    if (
        returncode == 0
        and dataset == "daily_kline"
        and source_policy == "bigqmt_primary"
    ):
        target_date = (
            date_str.strip()
            or datetime.now().strftime("%Y-%m-%d")
        )
        try:
            from server.common.batch_db import create_batch_engine
            from tools.attest_qmt_daily_kline import attest_range

            attestation = attest_range(
                create_batch_engine(future=True),
                start_date=target_date,
                end_date=target_date,
                apply=True,
            )
            if attestation.get("status") != "COMPLETED":
                returncode = 3
                error = (
                    "BigQMT daily attestation did not complete: "
                    f"{attestation.get('status')}"
                )
        except Exception as exc:
            returncode = 3
            error = f"BigQMT daily attestation failed: {exc}"
    return {
        "status": "success" if returncode == 0 else "failed",
        "dataset": dataset,
        "table": table_name,
        "returncode": returncode,
        "error": error,
        "minute_count": max(0, int(minute_count)),
        "start_date": start_date.strip(),
        "end_date": end_date.strip(),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "source_policy": source_policy,
        "attestation": attestation,
        "level1_capture": level1_capture,
    }


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    parser = argparse.ArgumentParser(description="Run canonical synchronization with signed-in BigQMT preferred")
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument("--date", default="")
    parser.add_argument("--minute-count", type=int, default=0)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = run_dataset(
        args.dataset,
        date_str=args.date.strip(),
        minute_count=args.minute_count,
        start_date=args.start_date.strip(),
        end_date=args.end_date.strip(),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(result)
    return 0 if result["status"] == "success" else int(result["returncode"] or 2)


if __name__ == "__main__":
    raise SystemExit(main())
