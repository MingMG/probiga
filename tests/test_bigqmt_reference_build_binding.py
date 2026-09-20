from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from integrations.bigqmt import reference
from server.common import qmt_edge_release_receipt, scheduler_runtime_health
from tools import run_big_qmt_bridge as consumer
from tools import sync_bigqmt_reference as membership


BUILD = "a" * 40
OTHER_BUILD = "b" * 40
IDENTITY_KEYS = (
    "PROBIGA_SCHEDULER_BUILD_SHA",
    "PROBIGA_BUILD_COMMIT_SHA",
    "PROBIGA_EXPECTED_GIT_SHA",
)


@pytest.fixture(autouse=True)
def clean_identity(monkeypatch):
    for key in IDENTITY_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def source(monkeypatch):
    native = SimpleNamespace(capabilities=MagicMock(return_value={"model_instance_id": "model-one"}))

    def validate(payload, *, expected_build_sha, **_kwargs):
        assert expected_build_sha == BUILD
        return {"compatible_app_build_sha": BUILD, "model": payload["model_instance_id"]}

    monkeypatch.setattr(reference, "validate_strategy_release_payload", validate)
    return native


@pytest.mark.parametrize("key", IDENTITY_KEYS)
def test_reference_capture_binds_each_formal_runtime_declaration(monkeypatch, source, key):
    monkeypatch.setenv(key, BUILD)
    captured = []
    result = reference.run_reference_capture(
        lambda session: captured.append(session.build_sha) or "complete", source_bridge=source,
    )
    assert result == "complete"
    assert captured == [BUILD]
    assert source.capabilities.call_count == 2


@pytest.mark.parametrize("value", ["", "0" * 40, "not-a-build", "a" * 39])
def test_missing_or_invalid_reference_identity_never_calls_qmt(source, value):
    with pytest.raises(RuntimeError, match="QMT_REFERENCE_RELEASE_BUILD_REQUIRED"):
        reference.run_reference_capture(
            lambda _session: pytest.fail("unbound capture ran"), expected_build_sha=value,
            source_bridge=source, recover_session=lambda: pytest.fail("identity error retried login"),
        )
    source.capabilities.assert_not_called()


@pytest.mark.parametrize("key", IDENTITY_KEYS)
def test_explicit_identity_cannot_override_conflicting_runtime(monkeypatch, source, key):
    monkeypatch.setenv(key, OTHER_BUILD)
    with pytest.raises(RuntimeError, match="QMT_REFERENCE_RELEASE_BUILD_CONFLICT"):
        reference.run_reference_capture(
            lambda _session: pytest.fail("conflicting capture ran"),
            expected_build_sha=BUILD, source_bridge=source,
        )
    source.capabilities.assert_not_called()


def test_runtime_identity_change_during_capture_discards_result(monkeypatch, source):
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD)

    def capture(_session):
        monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", OTHER_BUILD)
        return {"data": "must not publish"}

    with pytest.raises(RuntimeError, match="QMT_REFERENCE_RELEASE_BUILD_CONFLICT"):
        reference.run_reference_capture(capture, source_bridge=source)


def test_membership_fetch_passes_resolved_scheduler_build_explicitly(monkeypatch):
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD)
    boundaries = []

    def capture(callback, *, expected_build_sha):
        boundaries.append(expected_build_sha)
        return callback("bound-native-session")

    monkeypatch.setattr(membership, "run_reference_capture", capture)
    monkeypatch.setattr(membership, "_fetch_and_validate", lambda _engine, **kwargs: kwargs)
    result = membership.fetch_and_validate(object())
    assert boundaries == [BUILD]
    assert result["source_bridge"] == "bound-native-session"


@pytest.fixture
def active_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("BIG_QMT_MEMBERSHIP_CAPTURE_DIR", str(tmp_path / "captures"))
    state = {"build": BUILD, "activated": True, "ready": True, "host": consumer.socket.gethostname()}
    monkeypatch.setattr(consumer, "_git_head", lambda: state["build"])
    monkeypatch.setattr(membership, "validate_membership_publication_target",
                        lambda _engine, *, snapshot_date: snapshot_date)
    monkeypatch.setattr(qmt_edge_release_receipt, "check_qmt_edge_release_activation",
                        lambda _connection, *, expected_build_sha: (state["activated"], {}))

    def identity(_connection, *, expected_build_sha):
        assert expected_build_sha == BUILD
        return state["ready"], {"current": {"host_name": state["host"]}}

    monkeypatch.setattr(scheduler_runtime_health, "check_qmt_windows_edge_identity", identity)
    return state


@pytest.mark.parametrize("changed_key,changed_value,error", [
    ("activated", False, "RELEASE_NOT_ACTIVE"),
    ("ready", False, "EXECUTOR_IDENTITY_UNAVAILABLE"),
    ("host", "different-windows-host", "EXECUTOR_IDENTITY_UNAVAILABLE"),
])
def test_checkout_alone_never_authorizes_membership_capture(
    monkeypatch, active_runtime, changed_key, changed_value, error,
):
    frozen = consumer._freeze_reference_build()
    active_runtime[changed_key] = changed_value
    monkeypatch.setattr(membership, "fetch_and_validate", lambda *_a, **_k: pytest.fail("QMT was called"))
    monkeypatch.setattr(membership, "publish_verified_capture", lambda *_a, **_k: pytest.fail("unbound data published"))
    with pytest.raises(RuntimeError, match=error):
        consumer._run_membership_snapshot(MagicMock(), date(2026, 9, 18), expected_build_sha=frozen)


def test_consumer_startup_rejects_declared_checkout_conflict(monkeypatch, active_runtime):
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", OTHER_BUILD)
    with pytest.raises(RuntimeError, match="RELEASE_BUILD_CONFLICT"):
        consumer._freeze_reference_build()


@pytest.mark.parametrize("value", ["", "0" * 40, "unavailable"])
def test_consumer_cannot_freeze_missing_or_invalid_checkout(monkeypatch, value):
    monkeypatch.setattr(consumer, "_git_head", lambda: value)
    with pytest.raises(RuntimeError, match="RELEASE_BUILD_REQUIRED"):
        consumer._freeze_reference_build()


@pytest.mark.parametrize("change_during_capture", [False, True])
def test_running_consumer_never_relabels_itself_after_checkout_change(
    monkeypatch, active_runtime, change_during_capture,
):
    frozen = consumer._freeze_reference_build()
    reads = []

    def fetch(_engine, *, expected_build_sha, **_kwargs):
        reads.append(expected_build_sha)
        active_runtime["build"] = OTHER_BUILD
        return {}, {}

    monkeypatch.setattr(membership, "capture_to_store", fetch)
    monkeypatch.setattr(membership, "publish_verified_capture", lambda *_a, **_k: pytest.fail("changed-build data published"))
    if not change_during_capture:
        active_runtime["build"] = OTHER_BUILD
    with pytest.raises(RuntimeError, match="CHECKOUT_BUILD_CHANGED"):
        consumer._run_membership_snapshot(MagicMock(), date(2026, 9, 18), expected_build_sha=frozen)
    assert reads == ([BUILD] if change_during_capture else [])


def test_activation_revoked_during_capture_prevents_publication(monkeypatch, active_runtime):
    def fetch(_engine, **_kwargs):
        active_runtime["activated"] = False
        return {}, {}

    monkeypatch.setattr(membership, "capture_to_store", fetch)
    monkeypatch.setattr(membership, "publish_verified_capture", lambda *_a, **_k: pytest.fail("fenced data published"))
    with pytest.raises(RuntimeError, match="RELEASE_NOT_ACTIVE"):
        consumer._run_membership_snapshot(MagicMock(), date(2026, 9, 18), expected_build_sha=BUILD)


def test_active_frozen_consumer_publishes_only_after_verified_capture(monkeypatch, active_runtime):
    events = []

    def fetch(_engine, *, expected_build_sha, **_kwargs):
        assert expected_build_sha == BUILD
        events.append("capture")
        return {"native": "frames"}, {"rows": 1}

    def publish(_engine, capture, *, expected_build_sha):
        assert capture == ({"native": "frames"}, {"rows": 1})
        events.append("publish")
        return {"snapshot": {"status": "created"}, "membership_publication_receipt": {}}

    monkeypatch.setattr(membership, "capture_to_store", fetch)
    monkeypatch.setattr(membership, "publish_verified_capture", publish)
    monkeypatch.setattr("integrations.bigqmt.membership_checkpoint.MembershipCaptureStore.complete", lambda *_a: None)
    result = consumer._run_membership_snapshot(
        MagicMock(), date(2026, 9, 18), expected_build_sha=consumer._freeze_reference_build(),
    )
    assert events == ["capture", "publish"]
    assert result["snapshot"]["status"] == "created"
