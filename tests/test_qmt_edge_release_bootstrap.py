from __future__ import annotations

import copy
import json
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

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


def _release_request() -> dict[str, Any]:
    return receipt_contract.build_qmt_edge_release_request(
        build_sha=BUILD_SHA,
        requested_at=REQUESTED_AT,
    )


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
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def execute(
        self, statement: object, params: dict[str, Any] | None = None
    ) -> _MappedRows:
        self.statements.append((str(statement), dict(params or {})))
        return _MappedRows([])


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
    assert "if ($CurrentSha -ceq $TargetSha)" not in updater
    assert "--check-request" in updater
    migration = updater.index("backfill_guojin_qmt_local_history.py")
    request_check = updater.index("--check-request")
    assert migration < request_check
    assert "init --windows-local-option-file --json" in updater
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
