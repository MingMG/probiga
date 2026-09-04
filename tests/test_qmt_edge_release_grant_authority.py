from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tools import prepare_strategy_governance_schema as schema_tool
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap


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
    connection = engine.begin.return_value.__enter__.return_value
    events: list[tuple[str, object]] = []
    hold = {
        "build_sha": BUILD_SHA,
        "deployment_attempt_id": ATTEMPT_ID,
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
        "insert",
    ]
    assert all(observed is connection for _event, observed in events)
    assert result["activation_granted"] is True


def test_activation_grant_cli_never_uses_runtime_engine(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
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
        "append_release_activation_grant",
        lambda observed, **_kwargs: {
            "mode": "activation-grant",
            "status": "inserted",
            "build_sha": BUILD_SHA,
            "deployment_attempt_id": ATTEMPT_ID,
            "activation_granted": True,
            "database_writes": True,
            "engine_matches": observed is engine,
        },
    )

    result = bootstrap.main([
        "--activation-grant",
        "--expected-build-sha",
        BUILD_SHA,
        "--deployment-attempt-id",
        ATTEMPT_ID,
        "--compact",
    ])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["engine_matches"] is True
    engine.dispose.assert_called_once_with()
