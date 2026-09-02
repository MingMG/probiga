from __future__ import annotations

import base64
import copy
import json
import shutil
import subprocess
import sys
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from integrations.bigqmt.release_identity import (
    strategy_loaded_identity_sha256,
)
from server.common.qmt_attestation_contract import canonical_digest
from server.common import qmt_edge_release_receipt as receipt_contract
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap
from tools.sync_guojin_qmt_reference_data import (
    release_bound_reference_batch_id,
)


BUILD_SHA = "a" * 40
OTHER_BUILD_SHA = "b" * 40
HOST_NAME = "qmt-edge-host"
INSTANCE_ID = f"{HOST_NAME}-4321"
REQUESTED_AT = datetime(2026, 8, 25, 10, 0, 0)
CAPTURED_AT = REQUESTED_AT + timedelta(minutes=5)
REFERENCE_BATCH_ID = f"qmt_rel_{BUILD_SHA}_20260825100500"


def _reference_capture(*, complete: bool = False) -> dict[str, Any]:
    expected = ["000300.SH", "000905.SH"]
    successful = expected if complete else expected[:1]
    preserved = [] if complete else expected[1:]
    row_counts = {"000300.SH": 300, "000905.SH": 200 if complete else 0}
    returned_rows = sum(row_counts.values())
    table_status = (
        "REPLACED_QMT_ROWS_COMPLETE"
        if complete else "REPLACED_SUCCESSFUL_PARTITIONS"
    )
    catalog = {
        "schema": "probiga.test-catalog.v1",
        "batch_id": REFERENCE_BATCH_ID,
    }
    calendar = {
        "schema": "probiga.test-calendar.v1",
        "batch_id": REFERENCE_BATCH_ID,
    }
    return {
        "status": "success" if complete else "partial",
        "batch_id": REFERENCE_BATCH_ID,
        "release_build_sha": BUILD_SHA,
        "stock_catalog": catalog,
        "trade_calendar": calendar,
        "index_weight_publication": {
            "schema": "probiga.qmt-index-weight-publication-receipt.v1",
            "coverage_status": "COMPLETE" if complete else "PARTIAL",
            "coverage_complete": complete,
            "expected_index_count": len(expected),
            "successful_index_count": len(successful),
            "preserved_index_count": len(preserved),
            "coverage_ratio": len(successful) / len(expected),
            "expected_index_qmt_codes": expected,
            "successful_index_qmt_codes": successful,
            "preserved_index_qmt_codes": preserved,
            "per_index": [
                {
                    "index_qmt_code": symbol,
                    "index_code": symbol.split(".", 1)[0],
                    "returned_rows": row_counts[symbol],
                    "source_status": (
                        "NON_EMPTY" if symbol in successful
                        else "EMPTY_OR_FAILED"
                    ),
                    "publication_action": (
                        "REPLACE_PARTITION" if symbol in successful
                        else "PRESERVE_PREVIOUS_PARTITION"
                    ),
                }
                for symbol in expected
            ],
            "returned_rows": returned_rows,
            "publication_status": (
                "FULL_ATOMIC_REPLACE"
                if complete else "PARTIAL_ATOMIC_PARTITION_REPLACE"
            ),
            "publication_scope": (
                "ALL_QMT_INDEXES" if complete else "SUCCESSFUL_INDEXES"
            ),
            "atomic": True,
        },
        "tables": {
            "qmt_stock_catalog_batch": {
                "status": "INSERTED",
                **catalog,
                "manifest_hash": canonical_digest(catalog),
            },
            "qmt_trade_calendar_batch": {
                "status": "INSERTED",
                **calendar,
                "manifest_hash": canonical_digest(calendar),
            },
            "qmt_index_weight": {
                "status": table_status,
                "accepted_rows": returned_rows,
            },
            "si_index_constituent": {
                "status": table_status,
                "accepted_rows": returned_rows,
            },
        },
    }


def test_reference_capture_accepts_atomic_partial_partition_preservation() -> None:
    capture = _reference_capture()

    catalog, calendar, catalog_insert, calendar_insert, summary = (
        bootstrap._validated_reference_capture(
            capture,
            expected_build_sha=BUILD_SHA,
        )
    )

    assert catalog is capture["stock_catalog"]
    assert calendar is capture["trade_calendar"]
    assert catalog_insert is capture["tables"]["qmt_stock_catalog_batch"]
    assert calendar_insert is capture["tables"]["qmt_trade_calendar_batch"]
    assert summary["capture_status"] == "partial"
    index_summary = summary["index_weight_publication"]
    assert index_summary["coverage_status"] == "PARTIAL"
    assert index_summary["coverage_complete"] is False
    assert index_summary["publication_receipt_hash"] == canonical_digest(
        capture["index_weight_publication"]
    )


def test_reference_capture_accepts_strict_complete_index_publication() -> None:
    capture = _reference_capture(complete=True)

    bootstrap._validated_reference_capture(
        capture,
        expected_build_sha=BUILD_SHA,
    )


def test_reference_capture_rejects_unproven_partial_publications() -> None:
    cases: list[dict[str, Any]] = []

    non_atomic = _reference_capture()
    non_atomic["index_weight_publication"]["atomic"] = False
    cases.append(non_atomic)

    no_success = _reference_capture()
    publication = no_success["index_weight_publication"]
    publication.update({
        "coverage_status": "EMPTY",
        "successful_index_count": 0,
        "preserved_index_count": 2,
        "coverage_ratio": 0.0,
        "successful_index_qmt_codes": [],
        "preserved_index_qmt_codes": publication[
            "expected_index_qmt_codes"
        ],
        "returned_rows": 0,
    })
    for item in publication["per_index"]:
        item.update({
            "returned_rows": 0,
            "source_status": "EMPTY_OR_FAILED",
            "publication_action": "PRESERVE_PREVIOUS_PARTITION",
        })
    no_success["tables"]["qmt_index_weight"]["accepted_rows"] = 0
    no_success["tables"]["si_index_constituent"]["accepted_rows"] = 0
    cases.append(no_success)

    count_mismatch = _reference_capture()
    count_mismatch["index_weight_publication"]["preserved_index_count"] = 2
    cases.append(count_mismatch)

    destructive_empty_partition = _reference_capture()
    destructive_empty_partition["index_weight_publication"]["per_index"][1][
        "publication_action"
    ] = "REPLACE_PARTITION"
    cases.append(destructive_empty_partition)

    table_count_mismatch = _reference_capture()
    table_count_mismatch["tables"]["si_index_constituent"][
        "accepted_rows"
    ] -= 1
    cases.append(table_count_mismatch)

    non_finite_ratio = _reference_capture()
    non_finite_ratio["index_weight_publication"]["coverage_ratio"] = float("nan")
    cases.append(non_finite_ratio)

    boolean_table_count = _reference_capture()
    boolean_table_count["tables"]["qmt_index_weight"]["accepted_rows"] = True
    cases.append(boolean_table_count)

    for capture in cases:
        with pytest.raises(RuntimeError, match="reference capture is incomplete"):
            bootstrap._validated_reference_capture(
                capture,
                expected_build_sha=BUILD_SHA,
            )


def test_reference_capture_rejects_catalog_calendar_batch_drift() -> None:
    cases: list[dict[str, Any]] = []

    batch_drift = _reference_capture()
    batch_drift["trade_calendar"]["batch_id"] = "another-batch"
    cases.append(batch_drift)

    release_drift = _reference_capture()
    release_drift["release_build_sha"] = OTHER_BUILD_SHA
    cases.append(release_drift)

    unbound_batch = _reference_capture()
    unbound_batch["batch_id"] = f"qmt_rel_{BUILD_SHA}_bad-time"
    unbound_batch["stock_catalog"]["batch_id"] = unbound_batch["batch_id"]
    unbound_batch["trade_calendar"]["batch_id"] = unbound_batch["batch_id"]
    for name in ("qmt_stock_catalog_batch", "qmt_trade_calendar_batch"):
        unbound_batch["tables"][name]["batch_id"] = unbound_batch["batch_id"]
    cases.append(unbound_batch)

    manifest_hash_drift = _reference_capture()
    manifest_hash_drift["tables"]["qmt_stock_catalog_batch"][
        "manifest_hash"
    ] = "0" * 64
    cases.append(manifest_hash_drift)

    manifest_field_drift = _reference_capture()
    manifest_field_drift["tables"]["qmt_trade_calendar_batch"][
        "schema"
    ] = "probiga.drifted-calendar.v1"
    cases.append(manifest_field_drift)

    for capture in cases:
        with pytest.raises(RuntimeError, match="reference capture is incomplete"):
            bootstrap._validated_reference_capture(
                capture,
                expected_build_sha=BUILD_SHA,
            )


def _run_powershell_json(program: str) -> dict[str, Any]:
    encoded = base64.b64encode(program.encode("utf-16-le")).decode("ascii")
    executable = shutil.which("powershell.exe") or shutil.which("pwsh")
    assert executable is not None, "PowerShell is required for release tests"
    completed = subprocess.run(
        [
            executable,
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            encoded,
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip())


def _powershell_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _release_request() -> dict[str, Any]:
    return receipt_contract.build_qmt_edge_release_request(
        build_sha=BUILD_SHA,
        requested_at=REQUESTED_AT,
    )


def _release_request_row(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_uid": request["request_run_uid"],
        "status": "pending",
        "task_type": receipt_contract.QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
        "build_sha": request["build_sha"],
        "trigger_source": (
            receipt_contract.QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE
        ),
        "output": json.dumps(
            request,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }


def _release_receipt(
    *,
    requested_at: datetime = REQUESTED_AT,
    captured_at: datetime = CAPTURED_AT,
) -> dict[str, Any]:
    return receipt_contract.build_qmt_edge_release_receipt(
        build_sha=BUILD_SHA,
        request_run_uid=receipt_contract.qmt_edge_release_request_run_uid(
            BUILD_SHA
        ),
        requested_at=requested_at,
        host_name=HOST_NAME,
        scheduler_instance_id=INSTANCE_ID,
        catalog_batch_id=REFERENCE_BATCH_ID,
        catalog_manifest_hash="c" * 64,
        calendar_batch_id=REFERENCE_BATCH_ID,
        calendar_manifest_hash="d" * 64,
        calendar_start_date="1990-01-01",
        calendar_end_date="2027-12-31",
        local_history_schema_hash="e" * 64,
        qmt_capability_hash="f" * 64,
        captured_at=captured_at,
    )


class _ConnectionContext(AbstractContextManager[object]):
    def __init__(self, connection: object):
        self.connection = connection

    def __enter__(self) -> object:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class _ReadOnlyEngine:
    def __init__(self) -> None:
        self.connection = object()
        self.connect_calls = 0
        self.begin_calls = 0

    def connect(self) -> _ConnectionContext:
        self.connect_calls += 1
        return _ConnectionContext(self.connection)

    def begin(self) -> _ConnectionContext:
        self.begin_calls += 1
        return _ConnectionContext(self.connection)


class _MappedRows:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows

    def mappings(self) -> "_MappedRows":
        return self

    def all(self) -> list[dict[str, Any]]:
        return list(self.rows)


class _RecordingConnection:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self.rows = list(rows or [])

    def execute(
        self, statement: object, params: dict[str, Any] | None = None
    ) -> _MappedRows:
        self.statements.append((str(statement), dict(params or {})))
        return _MappedRows(self.rows)


def _forbidden(*_args: object, **_kwargs: object) -> Any:
    raise AssertionError("native QMT/sync path must not be called")


def test_release_request_is_canonical_and_exactly_build_bound() -> None:
    request = _release_request()
    unsigned = {
        key: value for key, value in request.items() if key != "request_hash"
    }

    assert request == {
        "schema": receipt_contract.QMT_EDGE_RELEASE_REQUEST_SCHEMA,
        "build_sha": BUILD_SHA,
        "request_run_uid": f"qmt-edge-request-{BUILD_SHA}",
        "requested_at": "2026-08-25T10:00:00",
        "request_hash": canonical_digest(unsigned),
    }
    assert receipt_contract.validate_qmt_edge_release_request(
        json.dumps(request, sort_keys=True),
        expected_build_sha=BUILD_SHA,
    ) == request

    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="content differs",
    ):
        receipt_contract.validate_qmt_edge_release_request(
            request,
            expected_build_sha=OTHER_BUILD_SHA,
        )

    tampered = copy.deepcopy(request)
    tampered["requested_at"] = "2026-08-25T10:00:01"
    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="content differs",
    ):
        receipt_contract.validate_qmt_edge_release_request(
            tampered,
            expected_build_sha=BUILD_SHA,
        )


def test_release_request_retry_returns_the_persisted_exact_request() -> None:
    persisted = _release_request()
    retry = receipt_contract.build_qmt_edge_release_request(
        build_sha=BUILD_SHA,
        requested_at=REQUESTED_AT + timedelta(minutes=5),
    )
    connection = _RecordingConnection([_release_request_row(persisted)])

    result = receipt_contract.insert_qmt_edge_release_request(
        connection,
        retry,
    )

    assert result == {"status": "idempotent", **persisted}
    assert result["requested_at"] == "2026-08-25T10:00:00"
    assert result["request_hash"] == persisted["request_hash"]
    assert len(connection.statements) == 1
    assert "INSERT INTO st_scheduled_task_history" not in (
        connection.statements[0][0]
    )


def test_append_release_request_retry_is_idempotent_at_tool_boundary() -> None:
    persisted = _release_request()
    connection = _RecordingConnection([_release_request_row(persisted)])
    engine = _ReadOnlyEngine()
    engine.connection = connection

    result = bootstrap.append_release_request(
        engine,
        expected_build_sha=BUILD_SHA,
        now=REQUESTED_AT + timedelta(minutes=5),
    )

    assert result == {
        "mode": "request",
        "status": "idempotent",
        **persisted,
        "database_writes": True,
    }
    assert engine.begin_calls == 1
    assert engine.connect_calls == 0
    assert len(connection.statements) == 1


def test_release_request_retry_revalidates_the_persisted_ledger_row() -> None:
    persisted = _release_request()
    row = _release_request_row(persisted)
    row["status"] = "success"
    connection = _RecordingConnection([row])

    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="ledger row differs",
    ):
        receipt_contract.insert_qmt_edge_release_request(
            connection,
            receipt_contract.build_qmt_edge_release_request(
                build_sha=BUILD_SHA,
                requested_at=REQUESTED_AT + timedelta(minutes=5),
            ),
        )


def test_release_request_retry_rejects_a_different_ledger_task_type() -> None:
    persisted = _release_request()
    row = _release_request_row(persisted)
    row["task_type"] = "another_task"
    connection = _RecordingConnection([row])

    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="ledger row differs",
    ):
        receipt_contract.insert_qmt_edge_release_request(
            connection,
            receipt_contract.build_qmt_edge_release_request(
                build_sha=BUILD_SHA,
                requested_at=REQUESTED_AT + timedelta(minutes=5),
            ),
        )


def test_release_request_rejects_malformed_or_zero_build_identity() -> None:
    for invalid in ("", "a" * 39, "g" * 40, "0" * 40):
        with pytest.raises(
            receipt_contract.QmtEdgeReleaseReceiptError,
            match="build_sha is invalid",
        ):
            receipt_contract.build_qmt_edge_release_request(
                build_sha=invalid,
                requested_at=REQUESTED_AT,
            )


def test_reference_batch_id_commits_full_build_and_fits_schema() -> None:
    batch_id = release_bound_reference_batch_id(
        BUILD_SHA,
        captured_at=datetime(2026, 8, 25, 10, 5, 0),
    )
    assert batch_id == REFERENCE_BATCH_ID
    assert len(batch_id) == 63
    for invalid in ("", "0" * 40, "g" * 40):
        with pytest.raises(ValueError, match="release_build_sha"):
            release_bound_reference_batch_id(invalid)


def test_release_order_works_outside_cron_and_linux_never_calls_qmt() -> None:
    deploy = (bootstrap.ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    updater = (
        bootstrap.ROOT / "tools" / "update_qmt_windows_edge.ps1"
    ).read_text(encoding="utf-8")

    schema = deploy.index("CUTOVER_STEP=prepare_strategy_governance_database_schema")
    request = deploy.index("CUTOVER_STEP=request_qmt_windows_edge_release_bootstrap")
    identity = deploy.index("CUTOVER_STEP=wait_for_qmt_windows_edge_identity")
    receipt = deploy.index("CUTOVER_STEP=wait_for_qmt_windows_edge_release_bootstrap")
    readiness = deploy.index(
        "CUTOVER_STEP=read_strategy_governance_qmt_history_readiness_after_schema"
    )
    assert schema < request < identity < receipt < readiness
    cutover = deploy[request:readiness]
    assert "sync_guojin_qmt_reference_data.py" not in cutover
    assert "--request --expected-build-sha" in cutover
    assert "--identity-only" in cutover
    assert "--release-bootstrap-only" in cutover
    assert "--writer-drain-timeout-seconds 660" in deploy

    # An arbitrary-time release does not wait for the three cron schedules.
    assert "--check-request" in updater
    assert "--check-ready" in updater
    assert "--check-strategy" in updater
    migration = updater.index("backfill_guojin_qmt_local_history.py")
    request_check = updater.index("--check-request")
    ready_preflight = updater.index(
        "Invoke-ReadOnlyStrategyPreflight $TargetSha"
    )
    ready_check = updater.index("--check-ready")
    ready_exit = updater.index("exit 0", updater.index("if ($ReadyExit -eq 0)"))
    migration_state = updater.index('$PreparedSha = ""')
    first_stop = updater.index("Stop-EdgeScheduler", updater.index("# Phase two"))
    fast_forward = updater.index('Invoke-Git @("merge", "--ff-only"')
    schema_validation_call = updater.index("$SchemaValidationOutput = &")
    authorization_failure = updater[
        updater.index("$AuthorizationExit = $LASTEXITCODE"):
        updater.index("if ($CurrentSha -cne $TargetSha)", request_check)
    ]
    assert request_check < first_stop
    assert request_check < fast_forward
    assert request_check < schema_validation_call
    assert "$TargetSha" in updater[request_check - 100:request_check + 100]
    assert "Stop-EdgeScheduler" not in authorization_failure
    assert 'Invoke-Git @("merge"' not in authorization_failure
    assert "$SchemaValidationOutput" not in authorization_failure
    assert "exit 0" in authorization_failure
    assert "not authorized or unavailable" in authorization_failure
    assert request_check < ready_check < ready_exit < first_stop < migration_state
    equal_sha_probe = updater[
        updater.index("if ($CurrentSha -ceq $TargetSha)"):
        updater.index("# Phase two")
    ]
    assert "$ReadyExit -ne 4" in equal_sha_probe
    assert "Stop-EdgeScheduler" not in equal_sha_probe
    assert migration < request_check
    assert "validate-schema --windows-local-option-file --json" in updater
    assert "init --windows-local-option-file --json" not in updater
    assert "$SchemaValidationExit -ne 0" in updater
    assert "dedicated privileged migration or boundary" in updater
    assert '"local-history-schema.sha"' in updater
    assert "$PreparedSha -cne $CurrentSha" in updater
    assert updater.index("$PreparedSha -cne $CurrentSha") > updater.index(
        'if ($CurrentSha -cne $TargetSha)'
    )
    assert "Move-Item -LiteralPath $MigrationReceiptTemp" in updater
    assert (
        "Remove-Item -LiteralPath $LocalHistoryMigrationReceipt" in updater
    )
    assert "Start-EdgeScheduler" in updater
    assert "--bootstrap --expected-build-sha" in updater
    strategy_reload = updater.index("$StrategyReloadOutput = &")
    strategy_preflight = updater.index(
        "Invoke-ReadOnlyStrategyPreflight $CurrentSha",
        migration_state,
    )
    strategy_probe = updater.index("--check-strategy", strategy_preflight)
    reload_arguments = updater.index("$StrategyReloadArguments = @(")
    reload_call = updater.index('"-ExpectedBuildSha", $CurrentSha', reload_arguments)
    scheduler_start = updater.index("Start-EdgeScheduler", strategy_reload)
    bootstrap_call = updater.index(
        "--bootstrap --expected-build-sha", scheduler_start
    )
    assert request_check < ready_preflight < ready_check
    assert request_check < first_stop < fast_forward < schema_validation_call
    assert (
        request_check
        < strategy_preflight
        < strategy_probe
        < reload_arguments
        < reload_call
        < strategy_reload
        < scheduler_start
        < bootstrap_call
    )
    assert "BigQMT exact strategy reloaded and identity-bound" in updater
    assert "$StrategyReloadExit -eq 3" in updater
    assert "NEEDS_USER_ACTION" in updater
    preflight_helper = updater[
        updater.index("function Invoke-ReadOnlyStrategyPreflight"):
        updater.index("function Invoke-Git")
    ]
    assert "-PreflightOnly" in preflight_helper
    assert "$PreflightExit -eq 3" in preflight_helper
    assert "[Console]::Out.WriteLine" in preflight_helper
    assert "Start-EdgeScheduler" not in preflight_helper
    assert "$BootstrapTool" not in preflight_helper
    assert updater.count("--check-request") == 1


def test_bootstrap_native_stderr_still_stops_edge_and_removes_receipt(
    tmp_path,
) -> None:
    updater = (
        bootstrap.ROOT / "tools" / "update_qmt_windows_edge.ps1"
    ).read_text(encoding="utf-8")
    block_start = updater.index("Start-EdgeScheduler\n$BootstrapExit = -1")
    block_end = updater.index(
        'Write-UpdateLog "release bootstrap ready',
        block_start,
    )
    bootstrap_block = updater[block_start:block_end]

    failing_bootstrap = tmp_path / "failing_bootstrap.py"
    failing_bootstrap.write_text(
        "import sys\n"
        "print('bootstrap native failure', file=sys.stderr)\n"
        "raise SystemExit(9)\n",
        encoding="utf-8",
    )
    migration_receipt = tmp_path / "local-history-schema.sha"
    migration_receipt.write_text(BUILD_SHA + "\n", encoding="ascii")

    program = f"""
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Events = [System.Collections.Generic.List[string]]::new()
function Start-EdgeScheduler {{ [void]$Events.Add("start") }}
function Stop-EdgeScheduler {{ [void]$Events.Add("stop") }}
function Write-UpdateLog([string]$Message) {{
    [void]$Events.Add("log:" + $Message)
}}
$PythonExe = {_powershell_literal(sys.executable)}
$BootstrapTool = {_powershell_literal(failing_bootstrap)}
$LocalHistoryMigrationReceipt = {_powershell_literal(migration_receipt)}
$CurrentSha = "{BUILD_SHA}"
$Caught = $false
$CaughtMessage = ""
try {{
{bootstrap_block}
}} catch {{
    $Caught = $true
    $CaughtMessage = [string]$_.Exception.Message
}}
[ordered]@{{
    caught = $Caught
    caught_message = $CaughtMessage
    bootstrap_exit = $BootstrapExit
    receipt_exists = Test-Path -LiteralPath $LocalHistoryMigrationReceipt
    events = @($Events)
}} | ConvertTo-Json -Compress
"""
    result = _run_powershell_json(program)

    assert result["caught"] is True
    assert result["caught_message"] == (
        "QMT Windows edge release bootstrap failed"
    )
    assert result["bootstrap_exit"] == 9
    assert result["receipt_exists"] is False
    assert result["events"][0:2] == ["start", "stop"]
    assert result["events"][2].startswith("log:release bootstrap failed")


def _bigqmt_strategy_release_payload(
    source_hash: str,
    *,
    build_sha: str = BUILD_SHA,
    git_blob: str = "c" * 40,
    artifact_hash: str = "e" * 64,
) -> dict[str, Any]:
    return {
        "status": "ok",
        "source": "gj_big_qmt_inner",
        "bridge_version": "bigqmt_inner_v2",
        "strategy_release_protocol": bootstrap.STRATEGY_RELEASE_PROTOCOL,
        "strategy_identity_protocol": (
            "probiga.bigqmt-loaded-strategy-identity.v1"
        ),
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_build_sha": build_sha,
        "strategy_git_blob": git_blob,
        "strategy_source_sha256": source_hash,
        "strategy_artifact_sha256": artifact_hash,
        "strategy_loaded_identity_sha256": strategy_loaded_identity_sha256(
            build_sha=build_sha,
            git_blob=git_blob,
            source_sha256=source_hash,
        ),
        "actions": ["current", "trading_calendar"],
        "native_capabilities": [
            {
                "capability": "trading_calendar",
                "action": "trading_calendar",
                "available": True,
                "source_method": "ContextInfo.get_trading_dates",
            },
            {
                "capability": "index_weight",
                "action": "index_members_many",
                "available": False,
                "source_method": "membership_only_no_native_weight",
            },
        ],
    }


def test_bigqmt_strategy_release_proof_binds_exact_source_and_native_calendar():
    source_hash = "d" * 64
    proof = bootstrap.validate_bigqmt_strategy_release(
        _bigqmt_strategy_release_payload(source_hash),
        expected_build_sha=BUILD_SHA,
        expected_source_sha256=source_hash,
        expected_git_blob="c" * 40,
        expected_artifact_sha256="e" * 64,
    )

    assert proof == {
        "schema": "probiga.bigqmt-strategy-release-proof.v2",
        "strategy_release_protocol": bootstrap.STRATEGY_RELEASE_PROTOCOL,
        "strategy_identity_protocol": (
            "probiga.bigqmt-loaded-strategy-identity.v1"
        ),
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_build_sha": BUILD_SHA,
        "strategy_git_blob": "c" * 40,
        "strategy_source_sha256": source_hash,
        "strategy_artifact_sha256": "e" * 64,
        "strategy_loaded_identity_sha256": strategy_loaded_identity_sha256(
            build_sha=BUILD_SHA,
            git_blob="c" * 40,
            source_sha256=source_hash,
        ),
        "trading_calendar": {
            "capability": "trading_calendar",
            "action": "trading_calendar",
            "available": True,
            "source_method": "ContextInfo.get_trading_dates",
        },
        "index_weight": {
            "capability": "index_weight",
            "action": "index_members_many",
            "available": False,
            "source_method": "membership_only_no_native_weight",
        },
    }


@pytest.mark.parametrize(
    "drift", ["hash", "build", "frozen", "calendar", "index_weight"]
)
def test_bigqmt_strategy_release_proof_fails_closed_on_drift(drift: str):
    source_hash = "d" * 64
    payload = _bigqmt_strategy_release_payload(source_hash)
    if drift == "hash":
        payload["strategy_source_sha256"] = "0" * 64
    elif drift == "build":
        payload["strategy_build_sha"] = OTHER_BUILD_SHA
    elif drift == "frozen":
        payload["strategy_identity_frozen"] = False
    elif drift == "calendar":
        payload["native_capabilities"][0]["available"] = False
    else:
        payload["native_capabilities"][1]["available"] = True

    with pytest.raises(RuntimeError, match="install and reload"):
        bootstrap.validate_bigqmt_strategy_release(
            payload,
            expected_build_sha=BUILD_SHA,
            expected_source_sha256=source_hash,
            expected_git_blob="c" * 40,
            expected_artifact_sha256="e" * 64,
        )


def test_release_receipt_binds_request_sha_instance_and_reference_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _release_receipt()
    monkeypatch.setattr(
        receipt_contract,
        "load_stock_catalog",
        lambda *_args, **_kwargs: SimpleNamespace(manifest_hash="c" * 64),
    )
    monkeypatch.setattr(
        receipt_contract,
        "load_trade_calendar_receipt",
        lambda *_args, **_kwargs: SimpleNamespace(manifest_hash="d" * 64),
    )
    monkeypatch.setattr(
        receipt_contract,
        "load_qmt_edge_release_request",
        lambda *_args, **_kwargs: {
            "request_run_uid": (
                receipt_contract.qmt_edge_release_request_run_uid(BUILD_SHA)
            ),
            "requested_at": "2026-08-25T10:00:00",
        },
    )

    assert receipt_contract.validate_qmt_edge_release_receipt(
        object(),
        json.dumps(value, sort_keys=True),
        expected_build_sha=BUILD_SHA,
        expected_host_name=HOST_NAME,
        expected_scheduler_instance_id=INSTANCE_ID,
    ) == value

    for kwargs, message in (
        ({"expected_build_sha": OTHER_BUILD_SHA}, "build differs"),
        ({"expected_host_name": "another-host"}, "host differs"),
        (
            {"expected_scheduler_instance_id": f"{HOST_NAME}-9999"},
            "instance differs",
        ),
    ):
        expected = {
            "expected_build_sha": BUILD_SHA,
            "expected_host_name": HOST_NAME,
            "expected_scheduler_instance_id": INSTANCE_ID,
            **kwargs,
        }
        with pytest.raises(
            receipt_contract.QmtEdgeReleaseReceiptError,
            match=message,
        ):
            receipt_contract.validate_qmt_edge_release_receipt(
                object(), value, **expected
            )

    tampered = copy.deepcopy(value)
    tampered["catalog_manifest_hash"] = "1" * 64
    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="hash/content differs",
    ):
        receipt_contract.validate_qmt_edge_release_receipt(
            object(),
            tampered,
            expected_build_sha=BUILD_SHA,
            expected_host_name=HOST_NAME,
            expected_scheduler_instance_id=INSTANCE_ID,
        )


def test_release_receipt_rejects_request_or_reference_identity_drift() -> None:
    common = {
        "build_sha": BUILD_SHA,
        "request_run_uid": receipt_contract.qmt_edge_release_request_run_uid(
            BUILD_SHA
        ),
        "requested_at": REQUESTED_AT,
        "host_name": HOST_NAME,
        "scheduler_instance_id": INSTANCE_ID,
        "catalog_batch_id": REFERENCE_BATCH_ID,
        "catalog_manifest_hash": "c" * 64,
        "calendar_batch_id": REFERENCE_BATCH_ID,
        "calendar_manifest_hash": "d" * 64,
        "calendar_start_date": "1990-01-01",
        "calendar_end_date": "2027-12-31",
        "local_history_schema_hash": "e" * 64,
        "qmt_capability_hash": "f" * 64,
        "captured_at": CAPTURED_AT,
    }
    cases = (
        (
            {"request_run_uid": f"qmt-edge-request-{OTHER_BUILD_SHA}"},
            "request_run_uid is invalid",
        ),
        (
            {"scheduler_instance_id": "another-host-4321"},
            "scheduler_instance_id is invalid",
        ),
        (
            {"catalog_batch_id": "qmt_reference_unbound"},
            "reference batch is not bound",
        ),
        (
            {"calendar_batch_id": f"{REFERENCE_BATCH_ID[:-1]}1"},
            "reference batch is not bound",
        ),
    )
    for changes, message in cases:
        with pytest.raises(
            receipt_contract.QmtEdgeReleaseReceiptError,
            match=message,
        ):
            receipt_contract.build_qmt_edge_release_receipt(
                **{**common, **changes}
            )


def test_receipt_insert_rejects_different_request_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _release_request()
    value = _release_receipt(
        requested_at=REQUESTED_AT + timedelta(seconds=1),
    )
    connection = _RecordingConnection()
    monkeypatch.setattr(
        receipt_contract,
        "load_qmt_edge_release_request",
        lambda *_args, **_kwargs: request,
    )
    monkeypatch.setattr(receipt_contract, "_reference_task_id", lambda _conn: 7)

    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="request binding differs",
    ):
        receipt_contract.insert_qmt_edge_release_receipt(connection, value)

    assert len(connection.statements) == 1
    assert "SELECT output FROM st_scheduled_task_history" in (
        connection.statements[0][0]
    )


def test_bootstrap_rejects_non_windows_before_database_or_qmt_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()

    with pytest.raises(RuntimeError, match="Windows-edge only"):
        bootstrap.run_release_bootstrap(
            engine,
            expected_build_sha=BUILD_SHA,
            platform_name="posix",
            git_head=BUILD_SHA,
            sync_runner=_forbidden,
            ping_runner=_forbidden,
            capabilities_runner=_forbidden,
        )

    assert engine.connect_calls == 0
    assert engine.begin_calls == 0


def test_existing_current_instance_receipt_is_idempotent_without_qmt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    request = _release_request()
    identity = {
        "current": {
            "host_name": HOST_NAME,
            "instance_id": INSTANCE_ID,
            "build_sha": BUILD_SHA,
        },
        "errors": [],
    }
    existing = {
        "status": "AVAILABLE",
        "receipt": _release_receipt(),
        "errors": [],
    }
    monkeypatch.setattr(
        bootstrap,
        "load_qmt_edge_release_request",
        lambda *_args, **_kwargs: request,
    )
    monkeypatch.setattr(
        bootstrap,
        "_wait_for_identity",
        lambda *_args, **_kwargs: identity,
    )

    def _existing_receipt(
        _connection: object,
        *,
        expected_build_sha: str,
        expected_poll_seconds: int,
    ) -> tuple[bool, dict[str, Any]]:
        assert expected_build_sha == BUILD_SHA
        assert expected_poll_seconds == 60
        return True, existing

    monkeypatch.setattr(
        bootstrap,
        "check_qmt_windows_edge_release_receipt",
        _existing_receipt,
    )

    result = bootstrap.run_release_bootstrap(
        engine,
        expected_build_sha=BUILD_SHA,
        expected_poll_seconds=60,
        platform_name="nt",
        host_name=HOST_NAME,
        git_head=BUILD_SHA,
        sync_runner=_forbidden,
        ping_runner=_forbidden,
        capabilities_runner=_forbidden,
    )

    assert result["status"] == "idempotent"
    assert result["expected_build_sha"] == BUILD_SHA
    assert result["database_writes"] is False
    assert result["qmt_calls"] is False
    assert result["identity"] == identity
    assert result["release_receipt"] == existing
    assert engine.connect_calls == 2
    assert engine.begin_calls == 0


def test_bootstrap_uses_runtime_visible_coverage_schema_after_privileged_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    identity = {
        "current": {
            "host_name": HOST_NAME,
            "instance_id": INSTANCE_ID,
            "build_sha": BUILD_SHA,
        },
        "errors": [],
    }
    coverage_calls: list[bool] = []

    monkeypatch.setattr(
        bootstrap,
        "load_qmt_edge_release_request",
        lambda *_args, **_kwargs: _release_request(),
    )
    monkeypatch.setattr(
        bootstrap,
        "_wait_for_identity",
        lambda *_args, **_kwargs: identity,
    )
    monkeypatch.setattr(
        bootstrap,
        "check_qmt_windows_edge_release_receipt",
        lambda *_args, **_kwargs: (
            False,
            {"status": "NOT_READY", "errors": ["receipt_missing"]},
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "validate_local_history_tables",
        lambda _engine: {"status": "ok"},
    )

    def _coverage(_connection: object, *, require_triggers: bool):
        coverage_calls.append(require_triggers)
        return {
            "physical_schema_verified": True,
            "physical_seal_verified": require_triggers,
        }

    monkeypatch.setattr(bootstrap, "validate_coverage_schema", _coverage)

    def _stop_after_coverage(*, timeout: int) -> dict[str, Any]:
        assert timeout == 180
        raise AssertionError("stop after coverage validation")

    with pytest.raises(AssertionError, match="stop after coverage validation"):
        bootstrap.run_release_bootstrap(
            engine,
            expected_build_sha=BUILD_SHA,
            local_engine=object(),
            ping_runner=_forbidden,
            capabilities_runner=_forbidden,
            bigqmt_capabilities_runner=_stop_after_coverage,
            platform_name="nt",
            host_name=HOST_NAME,
            git_head=BUILD_SHA,
        )

    assert coverage_calls == [False]
    assert engine.connect_calls == 3
    assert engine.begin_calls == 0


def test_exact_ready_probe_is_read_only_and_revalidates_live_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    receipt = {
        "status": "AVAILABLE",
        "receipt": _release_receipt(),
        "errors": [],
    }
    capabilities = {"status": "ok", "strategy_build_sha": BUILD_SHA}
    calls: list[object] = []

    monkeypatch.setattr(
        bootstrap,
        "check_qmt_windows_edge_release_receipt",
        lambda *_args, **_kwargs: (True, receipt),
    )

    def _capabilities(*, timeout: int) -> dict[str, Any]:
        calls.append(("capabilities", timeout))
        return capabilities

    def _validate(payload: dict[str, Any], *, expected_build_sha: str, **_kwargs):
        calls.append(("validate", payload, expected_build_sha))
        return {"strategy_build_sha": expected_build_sha}

    monkeypatch.setattr(bootstrap, "validate_bigqmt_strategy_release", _validate)
    result = bootstrap.check_existing_release_ready(
        engine,
        expected_build_sha=BUILD_SHA,
        expected_poll_seconds=60,
        bigqmt_capabilities_runner=_capabilities,
        platform_name="nt",
        git_head=BUILD_SHA,
    )

    assert result["status"] == "READY"
    assert result["database_writes"] is False
    assert result["qmt_calls"] is True
    assert result["release_receipt"] is receipt
    assert calls == [
        ("capabilities", 60),
        ("validate", capabilities, BUILD_SHA),
    ]
    assert engine.connect_calls == 1
    assert engine.begin_calls == 0


def test_loaded_strategy_probe_allows_exact_model_before_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    capabilities = {"status": "ok", "strategy_build_sha": BUILD_SHA}
    calls: list[object] = []

    def _capabilities(*, timeout: int) -> dict[str, Any]:
        calls.append(("capabilities", timeout))
        return capabilities

    def _validate(payload: dict[str, Any], *, expected_build_sha: str, **_kwargs):
        calls.append(("validate", payload, expected_build_sha))
        return {"strategy_build_sha": expected_build_sha}

    monkeypatch.setattr(bootstrap, "validate_bigqmt_strategy_release", _validate)
    result = bootstrap.check_loaded_strategy_ready(
        expected_build_sha=BUILD_SHA,
        bigqmt_capabilities_runner=_capabilities,
        platform_name="nt",
        git_head=BUILD_SHA,
    )

    assert result["status"] == "READY"
    assert result["database_writes"] is False
    assert result["qmt_calls"] is True
    assert calls == [
        ("capabilities", 60),
        ("validate", capabilities, BUILD_SHA),
    ]


def test_loaded_strategy_probe_reports_mismatch_without_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    monkeypatch.setattr(
        bootstrap,
        "validate_bigqmt_strategy_release",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("strategy build differs")
        ),
    )
    result = bootstrap.check_loaded_strategy_ready(
        expected_build_sha=BUILD_SHA,
        bigqmt_capabilities_runner=lambda **_kwargs: {
            "strategy_build_sha": "b" * 40
        },
        platform_name="nt",
        git_head=BUILD_SHA,
    )

    assert result["status"] == "NOT_READY"
    assert result["database_writes"] is False
    assert result["qmt_calls"] is True
    assert "strategy build differs" in result["strategy_error"]


def test_loaded_strategy_probe_transport_outage_is_not_reload_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )

    def _timeout(**_kwargs):
        raise TimeoutError("QMT IPC timeout")

    with pytest.raises(TimeoutError, match="QMT IPC timeout"):
        bootstrap.check_loaded_strategy_ready(
            expected_build_sha=BUILD_SHA,
            bigqmt_capabilities_runner=_timeout,
            platform_name="nt",
            git_head=BUILD_SHA,
        )


def test_not_ready_probe_never_calls_qmt_or_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    monkeypatch.setattr(
        bootstrap,
        "check_qmt_windows_edge_release_receipt",
        lambda *_args, **_kwargs: (
            False,
            {"status": "UNAVAILABLE", "errors": ["release_receipt_not_unique"]},
        ),
    )

    result = bootstrap.check_existing_release_ready(
        engine,
        expected_build_sha=BUILD_SHA,
        bigqmt_capabilities_runner=_forbidden,
        platform_name="nt",
        git_head=BUILD_SHA,
    )

    assert result["status"] == "NOT_READY"
    assert result["database_writes"] is False
    assert result["qmt_calls"] is False
    assert engine.connect_calls == 1
    assert engine.begin_calls == 0


def test_ready_probe_database_outage_does_not_become_reload_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    monkeypatch.setattr(
        bootstrap,
        "check_qmt_windows_edge_release_receipt",
        lambda *_args, **_kwargs: (
            False,
            {"status": "UNAVAILABLE", "errors": ["release_receipt_query_failed"]},
        ),
    )

    with pytest.raises(RuntimeError, match="database proof is unavailable"):
        bootstrap.check_existing_release_ready(
            engine,
            expected_build_sha=BUILD_SHA,
            bigqmt_capabilities_runner=_forbidden,
            platform_name="nt",
            git_head=BUILD_SHA,
        )

    assert engine.connect_calls == 1
    assert engine.begin_calls == 0


def test_missing_release_request_fails_closed_before_identity_or_qmt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge"
    )
    engine = _ReadOnlyEngine()
    identity_calls: list[bool] = []

    def _missing_request(*_args: object, **_kwargs: object) -> Any:
        raise receipt_contract.QmtEdgeReleaseReceiptError(
            "release request is unavailable"
        )

    def _unexpected_identity(*_args: object, **_kwargs: object) -> Any:
        identity_calls.append(True)
        raise AssertionError("identity/QMT work must follow a valid request")

    monkeypatch.setattr(
        bootstrap,
        "load_qmt_edge_release_request",
        _missing_request,
    )
    monkeypatch.setattr(bootstrap, "_wait_for_identity", _unexpected_identity)

    with pytest.raises(
        receipt_contract.QmtEdgeReleaseReceiptError,
        match="request is unavailable",
    ):
        bootstrap.run_release_bootstrap(
            engine,
            expected_build_sha=BUILD_SHA,
            platform_name="nt",
            host_name=HOST_NAME,
            git_head=BUILD_SHA,
            sync_runner=_forbidden,
            ping_runner=_forbidden,
            capabilities_runner=_forbidden,
        )

    assert identity_calls == []
    assert engine.connect_calls == 1
    assert engine.begin_calls == 0
