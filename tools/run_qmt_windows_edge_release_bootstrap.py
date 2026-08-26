#!/usr/bin/env python3
"""Coordinate one build-bound QMT Windows edge release bootstrap.

Linux may only append the request after the privileged additive schema cutover.
The Windows edge consumes that request, proves a fresh scheduler identity,
captures native QMT catalog/calendar evidence, and appends a separate audit
receipt.  No mode in this tool allows Linux to call QMT.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
from socket import gethostname
import subprocess
import sys
import time
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt import bridge as bigqmt_bridge
from integrations.bigqmt.release_identity import (
    STRATEGY_RELEASE_PROTOCOL,
    validate_strategy_release_payload,
)
from integrations.qmt import bridge
from integrations.qmt.local_history import (
    get_local_history_engine,
    validate_local_history_tables,
)
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_edge_release_receipt import (
    build_qmt_edge_release_receipt,
    build_qmt_edge_release_request,
    insert_qmt_edge_release_receipt,
    insert_qmt_edge_release_request,
    load_qmt_edge_release_request,
)
from server.common.qmt_history_coverage import validate_coverage_schema
from server.common.scheduler_runtime_health import (
    DEFAULT_SCHEDULER_POLL_SECONDS,
    QMT_WINDOWS_EDGE_ROLE,
    check_qmt_windows_edge_identity,
    check_qmt_windows_edge_release_receipt,
)


BIGQMT_STRATEGY_SOURCE = (
    ROOT
    / "integrations"
    / "bigqmt"
    / "qmt_strategy"
    / "probiga_big_qmt_bridge.py"
)


def validate_bigqmt_strategy_release(
    payload: dict[str, Any],
    *,
    expected_build_sha: str,
    expected_source_sha256: str | None = None,
    expected_git_blob: str | None = None,
    expected_artifact_sha256: str | None = None,
) -> dict[str, Any]:
    """Prove QMT is serving the exact build's frozen strategy bytes."""
    return validate_strategy_release_payload(
        payload,
        expected_build_sha=expected_build_sha,
        root=ROOT,
        source_path=BIGQMT_STRATEGY_SOURCE,
        expected_source_sha256=expected_source_sha256,
        expected_git_blob=expected_git_blob,
        expected_artifact_sha256=expected_artifact_sha256,
    )


def _git_head(root: Path = ROOT) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        text=True,
        encoding="utf-8",
        errors="strict",
        capture_output=True,
        check=True,
        timeout=30,
    )
    return result.stdout.strip().lower()


def append_release_request(
    engine: Any, *, expected_build_sha: str, now: datetime | None = None,
) -> dict[str, Any]:
    payload = build_qmt_edge_release_request(
        build_sha=expected_build_sha,
        requested_at=(now or datetime.now()).replace(microsecond=0),
    )
    with engine.begin() as connection:
        result = insert_qmt_edge_release_request(connection, payload)
    return {"mode": "request", **result, "database_writes": True}


def read_release_request(
    engine: Any, *, expected_build_sha: str,
) -> dict[str, Any]:
    with engine.connect() as connection:
        payload = load_qmt_edge_release_request(
            connection, expected_build_sha=expected_build_sha
        )
    return {"mode": "check-request", **payload, "database_writes": False}


def check_existing_release_ready(
    primary_engine: Any,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
    bigqmt_capabilities_runner: Callable[..., dict[str, Any]] = (
        bigqmt_bridge.capabilities
    ),
    platform_name: str | None = None,
    git_head: str | None = None,
) -> dict[str, Any]:
    """Read-only proof that this exact edge release is already healthy.

    The five-minute Windows updater calls this before stopping its scheduler or
    opening QMT.  A durable receipt alone is insufficient because the QMT model
    can be changed independently; the live model must also return the exact
    frozen build/source/artifact identity.
    """

    current_platform = os.name if platform_name is None else platform_name
    if current_platform != "nt":
        raise RuntimeError("QMT release readiness probe is Windows-edge only")
    if os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != QMT_WINDOWS_EDGE_ROLE:
        raise RuntimeError("QMT Windows edge executor role is not bound")
    observed_sha = str(git_head or _git_head()).strip().lower()
    expected_sha = str(expected_build_sha or "").strip().lower()
    if observed_sha != expected_sha:
        raise RuntimeError("QMT Windows edge checkout differs from requested build")

    with primary_engine.connect() as connection:
        ready, receipt = check_qmt_windows_edge_release_receipt(
            connection,
            expected_build_sha=expected_sha,
            expected_poll_seconds=expected_poll_seconds,
        )
    if not ready:
        errors = [str(item) for item in receipt.get("errors") or ()]
        if {"scheduler_runtime_query_failed", "release_receipt_query_failed"} & set(
            errors
        ):
            raise RuntimeError(
                "QMT exact-release readiness database proof is unavailable"
            )
        return {
            "mode": "check-ready",
            "status": "NOT_READY",
            "expected_build_sha": expected_sha,
            "release_receipt": receipt,
            "strategy_release": None,
            "database_writes": False,
            "qmt_calls": False,
        }

    capabilities = bigqmt_capabilities_runner(timeout=60)
    try:
        strategy_release = validate_bigqmt_strategy_release(
            capabilities,
            expected_build_sha=expected_sha,
        )
    except RuntimeError as exc:
        return {
            "mode": "check-ready",
            "status": "NOT_READY",
            "expected_build_sha": expected_sha,
            "release_receipt": receipt,
            "strategy_release": None,
            "strategy_error": str(exc),
            "database_writes": False,
            "qmt_calls": True,
        }
    return {
        "mode": "check-ready",
        "status": "READY",
        "expected_build_sha": expected_sha,
        "release_receipt": receipt,
        "strategy_release": strategy_release,
        "database_writes": False,
        "qmt_calls": True,
    }


def _wait_for_identity(
    engine: Any,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int,
    timeout_seconds: int,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    latest: dict[str, Any] = {}
    while True:
        with engine.connect() as connection:
            passed, latest = check_qmt_windows_edge_identity(
                connection,
                expected_build_sha=expected_build_sha,
                expected_poll_seconds=expected_poll_seconds,
            )
        if passed:
            return latest
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "fresh build-bound QMT Windows edge heartbeat is unavailable: "
                + ",".join(str(item) for item in latest.get("errors") or ())
            )
        sleep(min(5.0, max(0.0, deadline - time.monotonic())))


def run_release_bootstrap(
    primary_engine: Any,
    *,
    expected_build_sha: str,
    expected_poll_seconds: int = DEFAULT_SCHEDULER_POLL_SECONDS,
    heartbeat_timeout_seconds: int = 240,
    local_engine: Any | None = None,
    sync_runner: Callable[..., dict[str, Any]] | None = None,
    ping_runner: Callable[..., dict[str, Any]] = bridge.ping,
    capabilities_runner: Callable[..., dict[str, Any]] = bridge.capabilities,
    bigqmt_capabilities_runner: Callable[..., dict[str, Any]] = (
        bigqmt_bridge.capabilities
    ),
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    platform_name: str | None = None,
    host_name: str | None = None,
    git_head: str | None = None,
) -> dict[str, Any]:
    """Run only on the capable Windows edge after a Linux release request."""

    current_platform = os.name if platform_name is None else platform_name
    if current_platform != "nt":
        raise RuntimeError("QMT release bootstrap is Windows-edge only")
    if os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != QMT_WINDOWS_EDGE_ROLE:
        raise RuntimeError("QMT Windows edge executor role is not bound")
    observed_sha = str(git_head or _git_head()).strip().lower()
    expected_sha = str(expected_build_sha or "").strip().lower()
    if observed_sha != expected_sha:
        raise RuntimeError("QMT Windows edge checkout differs from requested build")

    with primary_engine.connect() as connection:
        request = load_qmt_edge_release_request(
            connection, expected_build_sha=expected_sha
        )
    identity = _wait_for_identity(
        primary_engine,
        expected_build_sha=expected_sha,
        expected_poll_seconds=expected_poll_seconds,
        timeout_seconds=heartbeat_timeout_seconds,
        sleep=sleep,
    )
    current = identity.get("current")
    if not isinstance(current, dict):
        raise RuntimeError("QMT Windows edge current identity is unavailable")
    expected_host = str(host_name or gethostname()).strip()
    if current.get("host_name") != expected_host:
        raise RuntimeError("QMT Windows edge heartbeat host differs")

    # A retry for the same live process is a read-only verification.  It must
    # not make another expensive native reference capture.
    with primary_engine.connect() as connection:
        already_ready, existing = check_qmt_windows_edge_release_receipt(
            connection,
            expected_build_sha=expected_sha,
            expected_poll_seconds=expected_poll_seconds,
        )
    if already_ready:
        return {
            "mode": "bootstrap",
            "status": "idempotent",
            "expected_build_sha": expected_sha,
            "identity": identity,
            "release_receipt": existing,
            "database_writes": False,
            "qmt_calls": False,
        }

    owned_local_engine = local_engine is None
    target_local_engine = local_engine or get_local_history_engine()
    try:
        local_schema = validate_local_history_tables(target_local_engine)
    finally:
        if owned_local_engine:
            target_local_engine.dispose()
    with primary_engine.connect() as connection:
        coverage_schema = validate_coverage_schema(
            connection, require_triggers=True
        )

    ping = ping_runner(timeout=60)
    capabilities = capabilities_runner(timeout=180)
    bigqmt_capabilities = bigqmt_capabilities_runner(timeout=180)
    ping_rows = ping.get("rows") if isinstance(ping, dict) else None
    capability_rows = (
        capabilities.get("rows") if isinstance(capabilities, dict) else None
    )
    if (
        ping.get("ok") is not True
        or not isinstance(ping_rows, list)
        or len(ping_rows) != 1
        or ping_rows[0].get("status") != "ok"
        or capabilities.get("ok") is not True
        or not isinstance(capability_rows, list)
        or not capability_rows
        or str(capabilities.get("provider") or "") != "gj_qmt"
    ):
        raise RuntimeError("QMT bridge capability proof is unavailable")
    bigqmt_strategy_release = validate_bigqmt_strategy_release(
        bigqmt_capabilities,
        expected_build_sha=expected_sha,
    )
    capability_evidence = {
        "schema": "probiga.qmt-windows-edge-capability-proof.v1",
        "ping": ping,
        "capabilities": capabilities,
        "bigqmt_strategy_release": bigqmt_strategy_release,
        "coverage_schema": coverage_schema,
    }

    if sync_runner is None:
        from tools.sync_guojin_qmt_reference_data import sync_reference_data

        sync_runner = sync_reference_data
    capture = sync_runner(
        start_year=1990,
        end_year=datetime.now().year + 1,
        iscomplete=True,
        refresh_timeout=900,
        skip_refresh=False,
        skip_calendar=False,
        dry_run=False,
        historical_instrument_archive="",
        release_build_sha=expected_sha,
    )
    tables = capture.get("tables") if isinstance(capture, dict) else None
    catalog = capture.get("stock_catalog") if isinstance(capture, dict) else None
    calendar = capture.get("trade_calendar") if isinstance(capture, dict) else None
    catalog_insert = tables.get("qmt_stock_catalog_batch") if isinstance(tables, dict) else None
    calendar_insert = tables.get("qmt_trade_calendar_batch") if isinstance(tables, dict) else None
    if (
        capture.get("status") != "success"
        or not isinstance(catalog, dict)
        or not isinstance(calendar, dict)
        or not isinstance(catalog_insert, dict)
        or not isinstance(calendar_insert, dict)
        or catalog.get("batch_id") != calendar.get("batch_id")
        or catalog_insert.get("status") != "INSERTED"
        or calendar_insert.get("status") != "INSERTED"
    ):
        raise RuntimeError("QMT immutable reference capture is incomplete")

    captured_at = (now or datetime.now()).replace(microsecond=0)
    receipt = build_qmt_edge_release_receipt(
        build_sha=expected_sha,
        request_run_uid=request["request_run_uid"],
        requested_at=request["requested_at"],
        host_name=str(current["host_name"]),
        scheduler_instance_id=str(current["instance_id"]),
        catalog_batch_id=str(catalog["batch_id"]),
        catalog_manifest_hash=str(catalog_insert["manifest_hash"]),
        calendar_batch_id=str(calendar["batch_id"]),
        calendar_manifest_hash=str(calendar_insert["manifest_hash"]),
        calendar_start_date=str(calendar["start_date"]),
        calendar_end_date=str(calendar["end_date"]),
        local_history_schema_hash=canonical_digest(local_schema),
        qmt_capability_hash=canonical_digest(capability_evidence),
        captured_at=captured_at,
    )
    with primary_engine.begin() as connection:
        inserted = insert_qmt_edge_release_receipt(connection, receipt)
    with primary_engine.connect() as connection:
        passed, verified = check_qmt_windows_edge_release_receipt(
            connection,
            expected_build_sha=expected_sha,
            expected_poll_seconds=expected_poll_seconds,
        )
    if not passed:
        raise RuntimeError("QMT release receipt readback failed")
    return {
        "mode": "bootstrap",
        "status": inserted["status"],
        "expected_build_sha": expected_sha,
        "identity": identity,
        "release_receipt": verified,
        "database_writes": True,
        "qmt_calls": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--request", action="store_true")
    modes.add_argument("--check-request", action="store_true")
    modes.add_argument("--check-ready", action="store_true")
    modes.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--expected-poll-seconds", type=int, default=60)
    parser.add_argument("--heartbeat-timeout-seconds", type=int, default=240)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args(argv)
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    expected_env_sha = os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "").strip().lower()
    if expected_env_sha and expected_env_sha != args.expected_build_sha.lower():
        raise SystemExit("expected build SHA differs from release environment")
    engine = create_tool_engine()
    try:
        if args.request:
            result = append_release_request(
                engine, expected_build_sha=args.expected_build_sha
            )
        elif args.check_request:
            result = read_release_request(
                engine, expected_build_sha=args.expected_build_sha
            )
        elif args.check_ready:
            result = check_existing_release_ready(
                engine,
                expected_build_sha=args.expected_build_sha,
                expected_poll_seconds=args.expected_poll_seconds,
            )
        else:
            result = run_release_bootstrap(
                engine,
                expected_build_sha=args.expected_build_sha,
                expected_poll_seconds=args.expected_poll_seconds,
                heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
            )
    except Exception as exc:
        result = {
            "status": "UNAVAILABLE",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
            "database_writes": False,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 2
    finally:
        engine.dispose()
    print(json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        indent=None if args.compact else 2,
        default=str,
    ))
    if args.check_ready and result.get("status") == "NOT_READY":
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
