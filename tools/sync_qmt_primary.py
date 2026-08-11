from __future__ import annotations

"""Run a canonical data job with signed-in BigQMT as the preferred source.

On the Windows QMT owner, the standard QMT built-in strategy is preferred for
market data. Public sources remain the protected fallback when BigQMT is not
enabled, while the legacy MiniQMT gateway is supported only as an explicit
compatibility option.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
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
            level1_capture = _run_bigqmt_level1_window(env)
            returncode = int(level1_capture.get("returncode") or 0)
            error = str(level1_capture.get("error") or "")
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
