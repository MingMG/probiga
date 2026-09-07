from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tools import prepare_strategy_governance_schema as schema_tool
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap
from server.common import qmt_edge_release_receipt as ledger


BUILD_SHA = "1" * 40
ATTEMPT_ID = "a" * 32
MIGRATOR_IDENTITY = "probiga_migrator@127.0.0.1"
RUNTIME_IDENTITY = "probiga_runtime@127.0.0.1"


class _MappingRows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


@pytest.mark.parametrize(
    ("os_name", "effective_uid"),
    (("nt", 0), ("posix", 1000)),
)
@pytest.mark.parametrize(
    ("grant_mode", "mode_arguments"),
    (
        (
            "--activation-grant",
            ["--deployment-attempt-id", ATTEMPT_ID],
        ),
        ("--activation-grant-latest", []),
        ("--request-compatibility-quiescence", ["--deployment-attempt-id", ATTEMPT_ID]),
        ("--request-recoverable-quiescence", ["--deployment-attempt-id", ATTEMPT_ID,
                                              "--target-build-sha", "2" * 40]),
        ("--request-forward-quiescence", ["--deployment-attempt-id", ATTEMPT_ID,
                                          "--prior-build-sha", "2" * 40]),
        ("--abort-precutover", ["--deployment-attempt-id", ATTEMPT_ID,
                               "--target-build-sha", "2" * 40]),
    ),
)
def test_activation_grant_cli_fails_before_env_or_engine_for_untrusted_os_user(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    os_name: str,
    effective_uid: int,
    grant_mode: str,
    mode_arguments: list[str],
) -> None:
    monkeypatch.setattr(bootstrap.os, "name", os_name)
    monkeypatch.setattr(
        bootstrap.os,
        "geteuid",
        lambda: effective_uid,
        raising=False,
    )
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    monkeypatch.setattr(
        "tools.env_config.load_project_env",
        lambda: pytest.fail("grant route loaded runtime environment"),
    )
    monkeypatch.setattr(
        "tools.env_config.create_tool_engine",
        lambda: pytest.fail("grant route created runtime engine"),
    )

    result = bootstrap.main([
        grant_mode,
        "--expected-build-sha",
        BUILD_SHA,
        *mode_arguments,
        "--compact",
    ])

    assert result == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "UNAVAILABLE"
    assert payload["error_type"] == "PermissionError"
    assert payload["database_writes"] is False


def test_grant_engine_reuses_protected_migrator_boundary_and_disposes_on_fault(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    option_file = tmp_path / "mysql-migrator.ini"
    tls_ca = tmp_path / "mysql84-ca.pem"
    option_file.write_text("protected", encoding="utf-8")
    tls_ca.write_text("ca", encoding="utf-8")
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    events: list[object] = []

    monkeypatch.setattr(bootstrap, "_require_activation_grant_root", lambda: None)
    monkeypatch.setattr(schema_tool, "_require_root_execution", lambda: None)
    monkeypatch.setattr(
        "tools.env_config.load_project_env",
        lambda: events.append("load_env"),
    )
    monkeypatch.setattr(
        schema_tool,
        "_read_option_credential",
        lambda path, *, expected_user: (
            events.append(("credential", path, expected_user))
            or SimpleNamespace(path=option_file)
        ),
    )
    monkeypatch.setattr(
        schema_tool,
        "_runtime_ssl_ca",
        lambda: events.append("tls") or tls_ca,
    )
    monkeypatch.setattr(
        schema_tool,
        "_create_migrator_engine",
        lambda credential, ca: (
            events.append(("engine", credential.path, ca)) or engine
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "_attest_activation_grant_connection",
        lambda observed: (
            events.append(("attest", observed))
            or (_ for _ in ()).throw(RuntimeError("identity differs"))
        ),
    )

    with pytest.raises(RuntimeError, match="identity differs"):
        bootstrap._create_activation_grant_engine()

    assert events[:3] == [
        "load_env",
        ("credential", schema_tool.MIGRATOR_OPTION_FILE, "probiga_migrator"),
        "tls",
    ]
    assert ("engine", option_file, tls_ca) in events
    assert ("attest", connection) in events
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize(
    ("current_identity", "session_identity", "database_name"),
    (
        (RUNTIME_IDENTITY, MIGRATOR_IDENTITY, "probiga"),
        (MIGRATOR_IDENTITY, RUNTIME_IDENTITY, "probiga"),
        (MIGRATOR_IDENTITY, MIGRATOR_IDENTITY, "other"),
    ),
)
def test_activation_grant_connection_rejects_any_exact_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
    current_identity: str,
    session_identity: str,
    database_name: str,
) -> None:
    state = object()
    validate = MagicMock()
    monkeypatch.setattr(schema_tool, "_read_sa_state", lambda _connection: state)
    monkeypatch.setattr(schema_tool, "_validate_target_state", validate)

    connection = MagicMock()
    connection.execute.return_value = _MappingRows([{
        "activation_grant_current_identity": current_identity,
        "activation_grant_session_identity": session_identity,
        "activation_grant_database_name": database_name,
    }])
    with pytest.raises(RuntimeError, match="identity differs"):
        bootstrap._attest_activation_grant_connection(connection)


def test_activation_grant_connection_accepts_only_exact_migrator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = object()
    validate = MagicMock()
    monkeypatch.setattr(schema_tool, "_read_sa_state", lambda _connection: state)
    monkeypatch.setattr(schema_tool, "_validate_target_state", validate)
    migrator_connection = MagicMock()
    migrator_connection.execute.return_value = _MappingRows([{
        "activation_grant_current_identity": MIGRATOR_IDENTITY,
        "activation_grant_session_identity": MIGRATOR_IDENTITY,
        "activation_grant_database_name": "probiga",
    }])
    bootstrap._attest_activation_grant_connection(migrator_connection)

    validate.assert_called_once_with(
        state,
        expected_user=MIGRATOR_IDENTITY,
        require_database=True,
        expected_trust=0,
        require_trigger_session=True,
    )


def test_grant_attests_the_same_transaction_before_any_ledger_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    events: list[tuple[str, object]] = []
    hold = {
        "build_sha": BUILD_SHA,
        "deployment_attempt_id": ATTEMPT_ID,
        "hold_hash": "f" * 64,
    }
    grant = {**hold, "grant": True}

    monkeypatch.setattr(
        bootstrap,
        "_attest_activation_grant_connection",
        lambda observed: events.append(("attest", observed)),
    )
    monkeypatch.setattr(
        bootstrap,
        "load_latest_qmt_edge_release_quiescence_hold",
        lambda observed, **_kwargs: (
            events.append(("load_hold", observed)) or hold
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "build_qmt_edge_release_activation_grant",
        lambda **_kwargs: grant,
    )
    monkeypatch.setattr(
        bootstrap.recovery, "latest_hold",
        lambda observed: events.append(("global_hold", observed)) or hold,
    )
    monkeypatch.setattr(
        bootstrap,
        "insert_qmt_edge_release_activation_grant",
        lambda observed, _grant: (
            events.append(("insert", observed))
            or {
                "status": "inserted",
                "build_sha": BUILD_SHA,
                "deployment_attempt_id": ATTEMPT_ID,
            }
        ),
    )

    result = bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_ID,
    )

    assert [event for event, _connection in events] == [
        "attest",
        "load_hold",
        "global_hold",
        "insert",
    ]
    assert all(observed is connection for _event, observed in events)
    assert result["activation_granted"] is True


@pytest.mark.parametrize("mode, function_name", [
    ("activation-grant", "append_release_activation_grant"),
    ("request-compatibility-quiescence", "append_release_request_with_quiescence"),
])
def test_activation_grant_cli_never_uses_runtime_engine(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: str,
    function_name: str,
) -> None:
    engine = MagicMock()
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    monkeypatch.setattr(
        bootstrap,
        "_create_activation_grant_engine",
        lambda: engine,
    )
    monkeypatch.setattr(
        "tools.env_config.load_project_env",
        lambda: pytest.fail("grant route loaded runtime environment"),
    )
    monkeypatch.setattr(
        "tools.env_config.create_tool_engine",
        lambda: pytest.fail("grant route created runtime engine"),
    )
    monkeypatch.setattr(
        bootstrap,
        function_name,
        lambda observed, **_kwargs: {
            "mode": "activation-grant",
            "status": "inserted",
            "build_sha": BUILD_SHA,
            "deployment_attempt_id": ATTEMPT_ID,
            "activation_granted": True,
            "database_writes": True,
            "engine_matches": observed is engine,
            "compatibility_argument": _kwargs.get("compatibility_install"),
        },
    )

    result = bootstrap.main([
        "--" + mode,
        "--expected-build-sha",
        BUILD_SHA,
        "--deployment-attempt-id",
        ATTEMPT_ID,
        "--compact",
    ])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["engine_matches"] is True
    if mode == "request-compatibility-quiescence":
        assert payload["compatibility_argument"] is True
    engine.dispose.assert_called_once_with()


def test_forward_cli_binds_wrapper_environment_to_target_and_validates_prior_separately(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = BUILD_SHA
    prior = "2" * 40
    protected_engine = MagicMock()
    runtime_engine = MagicMock()
    observed: dict[str, object] = {}
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", target)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", target)
    monkeypatch.setattr(bootstrap, "_create_activation_grant_engine", lambda: protected_engine)
    monkeypatch.setattr(bootstrap, "_create_recovery_runtime_engine", lambda: runtime_engine)

    def validate_prior(runtime, *, expected_build_sha):
        observed["prior_validation"] = {
            "runtime": runtime,
            "expected_build_sha": expected_build_sha,
            "build_commit_sha": bootstrap.os.environ["PROBIGA_BUILD_COMMIT_SHA"],
            "expected_git_sha": bootstrap.os.environ["PROBIGA_EXPECTED_GIT_SHA"],
        }
        return {"seal": "validated"}

    monkeypatch.setattr(
        ledger, "_validate_qmt_edge_release_activation_trigger_seal", validate_prior,
    )
    monkeypatch.setattr(
        bootstrap, "_assert_recovery_database_identity",
        lambda connection, seal: observed.update({
            "identity_connection": connection,
            "identity_seal": seal,
            "restored_build_commit_sha": bootstrap.os.environ["PROBIGA_BUILD_COMMIT_SHA"],
            "restored_expected_git_sha": bootstrap.os.environ["PROBIGA_EXPECTED_GIT_SHA"],
        }),
    )

    def append(engine, runtime, **kwargs):
        observed.update({"engine": engine, "runtime": runtime, **kwargs})
        bootstrap._attest_forward_prior_database(
            engine, runtime, prior_build_sha=kwargs["prior_build_sha"],
        )
        return {
            "mode": "request-forward-quiescence", "status": "inserted",
            "build_sha": target, "prior_build_sha": prior,
            "deployment_attempt_id": ATTEMPT_ID,
            "context": {"schema": bootstrap.recovery.FORWARD_CONTEXT_SCHEMA},
            "activation_granted": False, "database_writes": True,
        }

    monkeypatch.setattr(bootstrap, "append_forward_release_request", append)
    result = bootstrap.main([
        "--request-forward-quiescence", "--expected-build-sha", target,
        "--prior-build-sha", prior, "--deployment-attempt-id", ATTEMPT_ID,
        "--compact",
    ])

    assert result == 0
    assert observed["engine"] is protected_engine
    assert observed["runtime"] is runtime_engine
    assert observed["expected_build_sha"] == target
    assert observed["prior_build_sha"] == prior
    assert observed["deployment_attempt_id"] == ATTEMPT_ID
    assert observed["prior_validation"] == {
        "runtime": runtime_engine.connect.return_value.__enter__.return_value,
        "expected_build_sha": prior,
        "build_commit_sha": prior,
        "expected_git_sha": prior,
    }
    assert observed["identity_connection"] is protected_engine
    assert observed["identity_seal"] == {"seal": "validated"}
    assert observed["restored_build_commit_sha"] == target
    assert observed["restored_expected_git_sha"] == target
    assert json.loads(capsys.readouterr().out)["build_sha"] == target
    protected_engine.dispose.assert_called_once_with()
    runtime_engine.dispose.assert_called_once_with()


def test_forward_prior_attestation_restores_target_environment_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = BUILD_SHA
    prior = "2" * 40
    runtime_engine = MagicMock()
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", target)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", target)

    def reject_prior(_runtime, *, expected_build_sha):
        assert expected_build_sha == prior
        assert bootstrap.os.environ["PROBIGA_BUILD_COMMIT_SHA"] == prior
        assert bootstrap.os.environ["PROBIGA_EXPECTED_GIT_SHA"] == prior
        raise RuntimeError("prior seal rejected")

    monkeypatch.setattr(
        ledger, "_validate_qmt_edge_release_activation_trigger_seal", reject_prior,
    )
    monkeypatch.setattr(
        bootstrap, "_assert_recovery_database_identity",
        lambda *_args: pytest.fail("identity checked after rejected prior seal"),
    )

    with pytest.raises(RuntimeError, match="prior seal rejected"):
        bootstrap._attest_forward_prior_database(
            MagicMock(), runtime_engine, prior_build_sha=prior,
        )

    assert bootstrap.os.environ["PROBIGA_BUILD_COMMIT_SHA"] == target
    assert bootstrap.os.environ["PROBIGA_EXPECTED_GIT_SHA"] == target


def test_forward_cli_failure_mirrors_unavailable_receipt_to_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = BUILD_SHA
    prior = "2" * 40
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", target)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", target)
    monkeypatch.setattr(
        bootstrap, "_create_activation_grant_engine",
        lambda: (_ for _ in ()).throw(RuntimeError("forward diagnostic")),
    )

    result = bootstrap.main([
        "--request-forward-quiescence", "--expected-build-sha", target,
        "--prior-build-sha", prior, "--deployment-attempt-id", ATTEMPT_ID,
        "--compact",
    ])

    assert result == 2
    captured = capsys.readouterr()
    stdout_payload = json.loads(captured.out)
    assert json.loads(captured.err) == stdout_payload
    assert stdout_payload == {
        "status": "UNAVAILABLE", "error_type": "RuntimeError",
        "error": "forward diagnostic", "database_writes": False,
    }


def test_read_only_runtime_env_file_is_loaded_before_controller_engine(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("MYSQL_URL=mysql://production\n", encoding="utf-8")
    engine = MagicMock()
    events: list[object] = []
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setattr(
        "tools.env_config.load_project_env",
        lambda path: events.append(("env", path)),
    )
    monkeypatch.setattr(
        "tools.env_config.create_tool_engine",
        lambda: events.append("engine") or engine,
    )
    monkeypatch.setattr(
        bootstrap, "read_release_activation",
        lambda observed, **_kwargs: {
            "mode": "check-activation", "status": "PENDING",
            "build_sha": BUILD_SHA, "activation_granted": False,
            "database_writes": False, "engine_matches": observed is engine,
        },
    )

    result = bootstrap.main([
        "--check-activation", "--expected-build-sha", BUILD_SHA,
        "--runtime-env-file", str(env_file), "--compact",
    ])

    assert result == 4
    assert events == [("env", env_file), "engine"]
    assert json.loads(capsys.readouterr().out)["engine_matches"] is True
    engine.dispose.assert_called_once_with()
