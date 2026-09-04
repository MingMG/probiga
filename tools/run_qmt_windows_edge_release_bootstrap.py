#!/usr/bin/env python3
"""Coordinate one build-bound QMT Windows edge release bootstrap.

Linux appends a per-attempt quiescence hold before cutover, then the root broker
appends the matching activation grant only after the privileged schema cutover.
The Windows edge consumes that grant, proves a fresh scheduler identity,
captures native QMT catalog/calendar evidence, and appends a separate audit
receipt.  No mode in this tool allows Linux to call QMT.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
from socket import gethostname
import subprocess
import sys
import time
from typing import Any, Callable

from sqlalchemy import text


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
    build_qmt_edge_release_activation_grant,
    build_qmt_edge_release_quiescence_hold,
    build_qmt_edge_release_receipt,
    build_qmt_edge_release_request,
    check_qmt_edge_release_activation,
    insert_qmt_edge_release_activation_grant,
    insert_qmt_edge_release_quiescence_hold,
    insert_qmt_edge_release_receipt,
    insert_qmt_edge_release_request,
    load_existing_qmt_edge_release_request,
    load_latest_qmt_edge_release_quiescence_hold,
    load_qmt_edge_release_request,
    validate_qmt_edge_release_receipt,
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
INDEX_WEIGHT_PUBLICATION_RECEIPT_SCHEMA = (
    "probiga.qmt-index-weight-publication-receipt.v1"
)


def _require_activation_grant_root() -> None:
    """Keep the migrator credential unavailable to Windows/runtime callers."""

    if (
        os.name != "posix"
        or not hasattr(os, "geteuid")
        or os.geteuid() != 0
    ):
        raise PermissionError(
            "QMT edge activation grants require the root production broker"
        )


def _create_activation_grant_engine() -> Any:
    """Open and attest the fixed root-only production migrator identity."""

    _require_activation_grant_root()
    from tools.env_config import load_project_env
    from tools.prepare_strategy_governance_schema import (
        EXPECTED_MIGRATOR_USER,
        MIGRATOR_OPTION_FILE,
        _create_migrator_engine,
        _read_option_credential,
        _require_root_execution,
        _runtime_ssl_ca,
    )

    # Reuse the schema broker's protected option-file, fixed TLS endpoint and
    # complete MySQL 8.4 target attestation.  Root is checked before loading
    # project configuration so a Windows/runtime caller cannot even attempt to
    # open the protected migrator credential.
    _require_root_execution()
    load_project_env()
    credential = _read_option_credential(
        MIGRATOR_OPTION_FILE,
        expected_user=EXPECTED_MIGRATOR_USER.split("@", 1)[0],
    )
    ssl_ca = _runtime_ssl_ca()
    if credential.path.samefile(ssl_ca):
        raise RuntimeError("QMT edge activation credential aliases the TLS CA")
    engine = _create_migrator_engine(credential, ssl_ca)
    try:
        with engine.connect() as connection:
            _attest_activation_grant_connection(connection)
        return engine
    except BaseException:
        engine.dispose()
        raise


def _attest_activation_grant_connection(connection: Any) -> None:
    """Bind grant authority to the exact connection that will INSERT it."""

    from server.common.scheduler_task_history_schema import (
        QMT_EDGE_RELEASE_ACTIVATION_SESSION_USER,
    )
    from tools.prepare_strategy_governance_schema import (
        DATABASE_NAME,
        EXPECTED_MIGRATOR_USER,
        _read_sa_state,
        _validate_target_state,
    )

    if EXPECTED_MIGRATOR_USER != QMT_EDGE_RELEASE_ACTIVATION_SESSION_USER:
        raise RuntimeError("QMT edge activation migrator identity contract differs")
    state = _read_sa_state(connection)
    _validate_target_state(
        state,
        expected_user=EXPECTED_MIGRATOR_USER,
        require_database=True,
        expected_trust=0,
        require_trigger_session=True,
    )
    identities = connection.execute(text(
        "SELECT CURRENT_USER() AS activation_grant_current_identity, "
        "USER() AS activation_grant_session_identity, "
        "DATABASE() AS activation_grant_database_name"
    )).mappings().all()
    if len(identities) != 1:
        raise RuntimeError("QMT edge activation migrator identity is unavailable")
    identity = identities[0]
    if (
        str(identity.get("activation_grant_current_identity") or "")
        != EXPECTED_MIGRATOR_USER
        or str(identity.get("activation_grant_session_identity") or "")
        != EXPECTED_MIGRATOR_USER
        or str(identity.get("activation_grant_database_name") or "")
        != DATABASE_NAME
    ):
        raise RuntimeError("QMT edge activation migrator identity differs")


def _load_reusable_reference_capture(
    connection: Any,
    *,
    now: datetime,
) -> tuple[
    dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]
] | None:
    """Return the newest still-current immutable catalog/calendar receipt."""

    max_age_hours = max(
        1.0,
        float(os.environ.get("QMT_REFERENCE_REUSE_MAX_HOURS", "36")),
    )
    rows = connection.execute(
        text(
            "SELECT output FROM st_scheduled_task_history "
            "WHERE task_type='qmt_edge_release_bootstrap' "
            "AND status='success' ORDER BY finished_at DESC LIMIT 20"
        )
    ).mappings().all()
    for row in rows:
        try:
            payload = json.loads(str(row.get("output") or ""))
            captured_at = datetime.fromisoformat(str(payload["captured_at"]))
            age_hours = (now - captured_at).total_seconds() / 3600.0
            if age_hours < -0.1 or age_hours > max_age_hours:
                continue
            verified = validate_qmt_edge_release_receipt(
                connection,
                payload,
                expected_build_sha=str(payload["build_sha"]),
                expected_host_name=str(payload["host_name"]),
                expected_scheduler_instance_id=str(
                    payload["scheduler_instance_id"]
                ),
            )
            batch_id = str(verified["catalog_batch_id"])
            catalog = {"batch_id": batch_id}
            calendar = {
                "batch_id": batch_id,
                "start_date": str(verified["calendar_start_date"]),
                "end_date": str(verified["calendar_end_date"]),
            }
            catalog_insert = {
                "batch_id": batch_id,
                "manifest_hash": str(verified["catalog_manifest_hash"]),
            }
            calendar_insert = {
                "batch_id": batch_id,
                "manifest_hash": str(verified["calendar_manifest_hash"]),
            }
            summary = {
                "mode": "REUSED_VERIFIED",
                "batch_id": batch_id,
                "source_release_build_sha": str(verified["build_sha"]),
                "source_captured_at": str(verified["captured_at"]),
                "age_hours": round(max(0.0, age_hours), 3),
                "catalog_manifest_hash": str(
                    verified["catalog_manifest_hash"]
                ),
                "calendar_manifest_hash": str(
                    verified["calendar_manifest_hash"]
                ),
            }
            return catalog, calendar, catalog_insert, calendar_insert, summary
        except Exception:
            continue
    return None


def _validated_reference_capture(
    capture: Any,
    *,
    expected_build_sha: str,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    """Accept only a complete capture or a proven-safe partial index publish."""

    def incomplete() -> None:
        raise RuntimeError("QMT immutable reference capture is incomplete")

    if not isinstance(capture, dict):
        incomplete()
    tables = capture.get("tables")
    catalog = capture.get("stock_catalog")
    calendar = capture.get("trade_calendar")
    publication = capture.get("index_weight_publication")
    if not all(
        isinstance(value, dict)
        for value in (tables, catalog, calendar, publication)
    ):
        incomplete()
    catalog_insert = tables.get("qmt_stock_catalog_batch")
    calendar_insert = tables.get("qmt_trade_calendar_batch")
    raw_publish = tables.get("qmt_index_weight")
    business_publish = tables.get("si_index_constituent")
    if not all(
        isinstance(value, dict)
        for value in (
            catalog_insert,
            calendar_insert,
            raw_publish,
            business_publish,
        )
    ):
        incomplete()
    expected_sha = str(expected_build_sha or "").strip().lower()
    batch_values = [
        capture.get("batch_id"),
        catalog.get("batch_id"),
        calendar.get("batch_id"),
        catalog_insert.get("batch_id"),
        calendar_insert.get("batch_id"),
    ]
    if (
        re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None
        or expected_sha == "0" * 40
        or capture.get("release_build_sha") != expected_sha
        or any(not isinstance(value, str) for value in batch_values)
        or len(set(batch_values)) != 1
        or re.fullmatch(
            rf"qmt_rel_{re.escape(expected_sha)}_[0-9]{{14}}",
            batch_values[0],
        ) is None
        or catalog_insert.get("status") != "INSERTED"
        or calendar_insert.get("status") != "INSERTED"
        or publication.get("schema")
        != INDEX_WEIGHT_PUBLICATION_RECEIPT_SCHEMA
        or publication.get("atomic") is not True
    ):
        incomplete()

    def validate_manifest(
        manifest: dict[str, Any], inserted: dict[str, Any],
    ) -> str:
        manifest_hash = inserted.get("manifest_hash")
        inserted_manifest = {
            key: value
            for key, value in inserted.items()
            if key not in {"status", "manifest_hash"}
        }
        if (
            not isinstance(manifest_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", manifest_hash) is None
            or inserted_manifest != manifest
            or canonical_digest(manifest) != manifest_hash
        ):
            incomplete()
        return manifest_hash

    catalog_manifest_hash = validate_manifest(catalog, catalog_insert)
    calendar_manifest_hash = validate_manifest(calendar, calendar_insert)

    count_names = (
        "expected_index_count",
        "successful_index_count",
        "preserved_index_count",
        "returned_rows",
    )
    counts = {name: publication.get(name) for name in count_names}
    if any(type(value) is not int or value < 0 for value in counts.values()):
        incomplete()
    expected_count = counts["expected_index_count"]
    successful_count = counts["successful_index_count"]
    preserved_count = counts["preserved_index_count"]
    returned_rows = counts["returned_rows"]
    if (
        expected_count <= 0
        or successful_count <= 0
        or returned_rows <= 0
        or expected_count != successful_count + preserved_count
    ):
        incomplete()

    def symbol_list(name: str, expected_size: int) -> list[str]:
        value = publication.get(name)
        if (
            not isinstance(value, list)
            or len(value) != expected_size
            or any(
                not isinstance(item, str)
                or re.fullmatch(r"[0-9]{6}\.(?:SH|SZ|BJ)", item) is None
                for item in value
            )
            or len(set(value)) != len(value)
        ):
            incomplete()
        return value

    expected = symbol_list("expected_index_qmt_codes", expected_count)
    successful = symbol_list("successful_index_qmt_codes", successful_count)
    preserved = symbol_list("preserved_index_qmt_codes", preserved_count)
    if (
        set(successful) & set(preserved)
        or set(successful) | set(preserved) != set(expected)
    ):
        incomplete()

    per_index = publication.get("per_index")
    if not isinstance(per_index, list) or len(per_index) != expected_count:
        incomplete()
    successful_set = set(successful)
    preserved_set = set(preserved)
    observed: list[str] = []
    observed_rows = 0
    for item in per_index:
        if not isinstance(item, dict):
            incomplete()
        symbol = item.get("index_qmt_code")
        row_count = item.get("returned_rows")
        if (
            not isinstance(symbol, str)
            or item.get("index_code") != symbol.split(".", 1)[0]
            or type(row_count) is not int
            or row_count < 0
        ):
            incomplete()
        observed.append(symbol)
        observed_rows += row_count
        if symbol in successful_set:
            valid_partition = (
                row_count > 0
                and item.get("source_status") == "NON_EMPTY"
                and item.get("publication_action") == "REPLACE_PARTITION"
            )
        elif symbol in preserved_set:
            valid_partition = (
                row_count == 0
                and item.get("source_status") == "EMPTY_OR_FAILED"
                and item.get("publication_action")
                == "PRESERVE_PREVIOUS_PARTITION"
            )
        else:
            valid_partition = False
        if not valid_partition:
            incomplete()
    if observed != expected or observed_rows != returned_rows:
        incomplete()

    ratio = publication.get("coverage_ratio")
    if (
        isinstance(ratio, bool)
        or not isinstance(ratio, (int, float))
        or not math.isfinite(float(ratio))
        or abs(float(ratio) - successful_count / expected_count) > 1e-12
    ):
        incomplete()
    raw_accepted_rows = raw_publish.get("accepted_rows")
    business_accepted_rows = business_publish.get("accepted_rows")
    if (
        type(raw_accepted_rows) is not int
        or type(business_accepted_rows) is not int
    ):
        incomplete()
    if publication.get("coverage_complete") is True:
        valid_state = (
            capture.get("status") == "success"
            and successful_count == expected_count
            and preserved_count == 0
            and publication.get("coverage_status") == "COMPLETE"
            and publication.get("publication_status") == "FULL_ATOMIC_REPLACE"
            and publication.get("publication_scope") == "ALL_QMT_INDEXES"
            and raw_publish.get("status") == "REPLACED_QMT_ROWS_COMPLETE"
            and business_publish.get("status")
            == "REPLACED_QMT_ROWS_COMPLETE"
        )
    elif publication.get("coverage_complete") is False:
        valid_state = (
            capture.get("status") == "partial"
            and successful_count < expected_count
            and preserved_count > 0
            and publication.get("coverage_status") == "PARTIAL"
            and publication.get("publication_status")
            == "PARTIAL_ATOMIC_PARTITION_REPLACE"
            and publication.get("publication_scope") == "SUCCESSFUL_INDEXES"
            and raw_publish.get("status")
            == "REPLACED_SUCCESSFUL_PARTITIONS"
            and business_publish.get("status")
            == "REPLACED_SUCCESSFUL_PARTITIONS"
        )
    else:
        valid_state = False
    if (
        not valid_state
        or raw_accepted_rows != returned_rows
        or business_accepted_rows != returned_rows
    ):
        incomplete()
    summary = {
        "capture_status": capture["status"],
        "batch_id": batch_values[0],
        "catalog_manifest_hash": catalog_manifest_hash,
        "calendar_manifest_hash": calendar_manifest_hash,
        "index_weight_publication": {
            key: publication[key]
            for key in (
                "schema",
                "coverage_status",
                "coverage_complete",
                "expected_index_count",
                "successful_index_count",
                "preserved_index_count",
                "coverage_ratio",
                "returned_rows",
                "publication_status",
                "publication_scope",
                "atomic",
            )
        } | {
            "publication_receipt_hash": canonical_digest(publication),
            "qmt_index_weight_accepted_rows": raw_accepted_rows,
            "si_index_constituent_accepted_rows": business_accepted_rows,
        },
    }
    return catalog, calendar, catalog_insert, calendar_insert, summary


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


def append_release_request_with_quiescence(
    engine: Any,
    *,
    expected_build_sha: str,
    deployment_attempt_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Atomically publish a per-attempt hold and the legacy SHA request.

    The hold is inserted first.  The legacy request is inserted second in the
    same transaction so an old updater can discover the target SHA, while a
    new updater can distinguish permission to quiesce from permission to run.
    """

    requested_at = (now or datetime.now()).replace(microsecond=0)
    with engine.begin() as connection:
        request = load_existing_qmt_edge_release_request(
            connection,
            expected_build_sha=expected_build_sha,
        )
        request_exists = request is not None
        if request is None:
            request = build_qmt_edge_release_request(
                build_sha=expected_build_sha,
                requested_at=requested_at,
            )
        hold = build_qmt_edge_release_quiescence_hold(
            build_sha=expected_build_sha,
            deployment_attempt_id=deployment_attempt_id,
            requested_at=requested_at,
        )
        hold_result = insert_qmt_edge_release_quiescence_hold(connection, hold)
        request_result = (
            {"status": "idempotent", **request}
            if request_exists
            else insert_qmt_edge_release_request(connection, request)
        )
    status = (
        "idempotent"
        if hold_result["status"] == request_result["status"] == "idempotent"
        else "inserted"
    )
    return {
        "mode": "request-quiescence",
        "status": status,
        "build_sha": hold_result["build_sha"],
        "deployment_attempt_id": hold_result["deployment_attempt_id"],
        "request_run_uid": request_result["request_run_uid"],
        "hold_run_uid": hold_result["hold_run_uid"],
        "hold_hash": hold_result["hold_hash"],
        "release_request_status": request_result["status"],
        "quiescence_hold_status": hold_result["status"],
        "activation_granted": False,
        "real_order": False,
        "database_writes": True,
    }


def _append_release_activation_grant(
    engine: Any,
    *,
    expected_build_sha: str,
    deployment_attempt_id: str | None,
    mode: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    with engine.begin() as connection:
        _attest_activation_grant_connection(connection)
        hold = load_latest_qmt_edge_release_quiescence_hold(
            connection,
            expected_build_sha=expected_build_sha,
            expected_deployment_attempt_id=deployment_attempt_id,
        )
        grant = build_qmt_edge_release_activation_grant(
            hold=hold,
            granted_at=(now or datetime.now()).replace(microsecond=0),
        )
        result = insert_qmt_edge_release_activation_grant(connection, grant)
    return {
        "mode": mode,
        **result,
        "activation_granted": True,
        "database_writes": True,
    }


def append_release_activation_grant(
    engine: Any,
    *,
    expected_build_sha: str,
    deployment_attempt_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Authorize only the explicitly named newest hold after schema cutover."""

    return _append_release_activation_grant(
        engine,
        expected_build_sha=expected_build_sha,
        deployment_attempt_id=deployment_attempt_id,
        mode="activation-grant",
        now=now,
    )


def append_latest_release_activation_grant(
    engine: Any,
    *,
    expected_build_sha: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Recoverably grant the latest hold when the attempt ID was lost."""

    return _append_release_activation_grant(
        engine,
        expected_build_sha=expected_build_sha,
        deployment_attempt_id=None,
        mode="activation-grant-latest",
        now=now,
    )


def read_release_activation(
    engine: Any,
    *,
    expected_build_sha: str,
    expected_deployment_attempt_id: str | None = None,
) -> dict[str, Any]:
    with engine.connect() as connection:
        _ready, detail = check_qmt_edge_release_activation(
            connection,
            expected_build_sha=expected_build_sha,
            expected_deployment_attempt_id=expected_deployment_attempt_id,
        )
    return {
        "mode": "check-activation",
        **detail,
        "database_writes": False,
    }


def read_release_request(
    engine: Any, *, expected_build_sha: str,
) -> dict[str, Any]:
    with engine.connect() as connection:
        payload = load_qmt_edge_release_request(
            connection, expected_build_sha=expected_build_sha
        )
    return {"mode": "check-request", **payload, "database_writes": False}


def check_loaded_strategy_ready(
    *,
    expected_build_sha: str,
    bigqmt_capabilities_runner: Callable[..., dict[str, Any]] = (
        bigqmt_bridge.capabilities
    ),
    platform_name: str | None = None,
    git_head: str | None = None,
) -> dict[str, Any]:
    """Read-only proof that QMT already runs the requested exact strategy.

    This probe intentionally does not require a release receipt.  It lets the
    updater resume after a user completed an interactive QMT reload while the
    scheduler was paused, avoiding a second UI stop/reopen cycle before the
    immutable receipt can be bootstrapped.
    """

    current_platform = os.name if platform_name is None else platform_name
    if current_platform != "nt":
        raise RuntimeError("QMT strategy readiness probe is Windows-edge only")
    if os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != QMT_WINDOWS_EDGE_ROLE:
        raise RuntimeError("QMT Windows edge executor role is not bound")
    observed_sha = str(git_head or _git_head()).strip().lower()
    expected_sha = str(expected_build_sha or "").strip().lower()
    if observed_sha != expected_sha:
        raise RuntimeError("QMT Windows edge checkout differs from requested build")
    # Transport/IPC failure is not evidence that the loaded identity differs
    # and therefore must not authorize an interactive stop/reload.  Let it
    # propagate so the CLI returns its fail-closed unavailable exit code.
    capabilities = bigqmt_capabilities_runner(timeout=60)
    try:
        strategy_release = validate_bigqmt_strategy_release(
            capabilities,
            expected_build_sha=expected_sha,
        )
    except Exception as exc:
        return {
            "mode": "check-strategy",
            "status": "NOT_READY",
            "expected_build_sha": expected_sha,
            "strategy_release": None,
            "strategy_error": str(exc)[:500],
            "database_writes": False,
            "qmt_calls": True,
        }
    return {
        "mode": "check-strategy",
        "status": "READY",
        "expected_build_sha": expected_sha,
        "strategy_release": strategy_release,
        "database_writes": False,
        "qmt_calls": True,
    }


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
        # The Windows edge deliberately connects as the least-privilege
        # runtime identity.  MySQL hides information_schema.TRIGGERS rows from
        # an account without TRIGGER, so asking this connection to re-attest
        # the privileged seal produces a false "inventory differs" failure.
        # Linux installs and validates the four immutable triggers before it
        # appends the build-bound request consumed above; here the edge proves
        # the complete runtime-visible table/index/foreign-key contract.
        coverage_schema = validate_coverage_schema(
            connection, require_triggers=False
        )

    bigqmt_capabilities = bigqmt_capabilities_runner(timeout=180)
    bigqmt_strategy_release = validate_bigqmt_strategy_release(
        bigqmt_capabilities,
        expected_build_sha=expected_sha,
    )
    # MiniQMT is being retired by the broker.  Keep its probe as compatibility
    # evidence only; the release authority is the build-bound BigQMT strategy.
    try:
        ping = ping_runner(timeout=60)
        capabilities = capabilities_runner(timeout=180)
        miniqmt_compatibility = {
            "status": "AVAILABLE",
            "ping": ping,
            "capabilities": capabilities,
        }
    except Exception as exc:
        miniqmt_compatibility = {
            "status": "UNAVAILABLE",
            "error_type": type(exc).__name__,
        }
    capability_evidence = {
        "schema": "probiga.qmt-windows-edge-capability-proof.v1",
        "reference_provider": "BIGQMT_STRATEGY",
        "miniqmt_compatibility": miniqmt_compatibility,
        "bigqmt_strategy_release": bigqmt_strategy_release,
        "coverage_schema": coverage_schema,
    }

    reference = None
    captured_at = (now or datetime.now()).replace(microsecond=0)
    if sync_runner is None:
        with primary_engine.connect() as connection:
            reference = _load_reusable_reference_capture(
                connection,
                now=captured_at,
            )
    if reference is None:
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
        reference = _validated_reference_capture(
            capture,
            expected_build_sha=expected_sha,
        )
    catalog, calendar, catalog_insert, calendar_insert, capture_summary = reference
    capability_evidence["reference_capture"] = capture_summary

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
    # Prove that the just-built receipt resolves to the actual immutable
    # catalog/calendar rows before appending the irreversible audit ledger
    # row.  A bad reference can then be retried instead of poisoning this
    # build/instance's idempotent receipt identity.
    with primary_engine.connect() as connection:
        validate_qmt_edge_release_receipt(
            connection,
            receipt,
            expected_build_sha=expected_sha,
            expected_host_name=str(current["host_name"]),
            expected_scheduler_instance_id=str(current["instance_id"]),
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
        "reference_capture": capture_summary,
        "database_writes": True,
        "qmt_calls": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--request", action="store_true")
    modes.add_argument("--request-quiescence", action="store_true")
    modes.add_argument("--activation-grant", action="store_true")
    modes.add_argument("--activation-grant-latest", action="store_true")
    modes.add_argument("--check-activation", action="store_true")
    modes.add_argument("--check-request", action="store_true")
    modes.add_argument("--check-ready", action="store_true")
    modes.add_argument("--check-strategy", action="store_true")
    modes.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--deployment-attempt-id")
    parser.add_argument("--expected-poll-seconds", type=int, default=60)
    parser.add_argument("--heartbeat-timeout-seconds", type=int, default=240)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args(argv)
    if (
        args.request_quiescence or args.activation_grant
    ) and not args.deployment_attempt_id:
        parser.error(
            "--deployment-attempt-id is required for this coordination mode"
        )
    if (
        args.deployment_attempt_id
        and not (
            args.request_quiescence
            or args.activation_grant
            or args.check_activation
        )
    ):
        parser.error(
            "--deployment-attempt-id is only valid for coordination modes"
        )
    from tools.env_config import create_tool_engine, load_project_env

    engine = None
    try:
        if args.activation_grant or args.activation_grant_latest:
            engine = _create_activation_grant_engine()
        else:
            load_project_env()
            engine = create_tool_engine()
        expected_env_sha = (
            os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "").strip().lower()
        )
        if expected_env_sha and expected_env_sha != args.expected_build_sha.lower():
            raise RuntimeError(
                "expected build SHA differs from release environment"
            )
        if args.request:
            result = append_release_request(
                engine, expected_build_sha=args.expected_build_sha
            )
        elif args.request_quiescence:
            result = append_release_request_with_quiescence(
                engine,
                expected_build_sha=args.expected_build_sha,
                deployment_attempt_id=args.deployment_attempt_id,
            )
        elif args.activation_grant:
            result = append_release_activation_grant(
                engine,
                expected_build_sha=args.expected_build_sha,
                deployment_attempt_id=args.deployment_attempt_id,
            )
        elif args.activation_grant_latest:
            result = append_latest_release_activation_grant(
                engine,
                expected_build_sha=args.expected_build_sha,
            )
        elif args.check_activation:
            result = read_release_activation(
                engine,
                expected_build_sha=args.expected_build_sha,
                expected_deployment_attempt_id=args.deployment_attempt_id,
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
        elif args.check_strategy:
            result = check_loaded_strategy_ready(
                expected_build_sha=args.expected_build_sha,
            )
        else:
            activation = read_release_activation(
                engine,
                expected_build_sha=args.expected_build_sha,
            )
            if activation.get("status") != "READY":
                result = {
                    **activation,
                    "mode": "bootstrap",
                    "qmt_calls": False,
                }
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
        if engine is not None:
            engine.dispose()
    print(json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        indent=None if args.compact else 2,
        default=str,
    ))
    if (
        (args.check_ready or args.check_strategy)
        and result.get("status") == "NOT_READY"
    ):
        return 4
    if result.get("status") == "PENDING":
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
