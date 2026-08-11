from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from tools import manage_v2_authority_registry as cli


SERVER_UUID = "123e4567-e89b-12d3-a456-426614174000"


def _trust_document() -> dict[str, object]:
    public_key = bytes(range(32))
    return {
        "operation": "TRUST_KEY",
        "payload": {
            "source_provider": "preview-provider",
            "key_id": "preview-key",
            "key_version": "v1",
            "public_key_base64url": base64.urlsafe_b64encode(public_key)
            .decode("ascii")
            .rstrip("="),
            "valid_from": "2026-08-04T00:00:00.000000+00:00",
            "valid_to": "2026-09-04T00:00:00.000000+00:00",
        },
    }


def _operation_file(tmp_path, document: dict[str, object] | None = None):
    path = tmp_path / "operation.json"
    path.write_text(
        json.dumps(document or _trust_document(), separators=(",", ":")),
        encoding="utf-8",
    )
    return path


def test_default_preview_validates_without_resolving_or_connecting(
    tmp_path, monkeypatch, capsys
) -> None:
    operation = _operation_file(tmp_path)
    monkeypatch.setattr(
        cli,
        "_resolve_apply_target",
        lambda *args, **kwargs: pytest.fail("preview resolved a database target"),
    )
    monkeypatch.setattr(
        cli,
        "create_engine",
        lambda *args, **kwargs: pytest.fail("preview opened a database engine"),
    )

    assert cli.main([str(operation)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["mode"] == "PREVIEW"
    assert output["database_connection_attempted"] is False
    assert output["production_activation_allowed"] is False
    assert output["actionable_output_allowed"] is False
    assert "public_key" not in output["preview"]
    assert "public_key_hash" in output["preview"]


def test_parser_rejects_unknown_fields_and_duplicate_json_keys(tmp_path) -> None:
    document = _trust_document()
    document["unexpected"] = True
    with pytest.raises(cli.AuthorityRegistryInputError, match="fields differ"):
        cli.parse_operation(str(_operation_file(tmp_path, document)))

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"operation":"TRUST_KEY","operation":"RECEIPT","payload":{}}',
        encoding="utf-8",
    )
    with pytest.raises(cli.AuthorityRegistryInputError, match="duplicate JSON key"):
        cli.parse_operation(str(duplicate))


def test_apply_requires_paired_explicit_test_or_ci_switch(tmp_path, capsys) -> None:
    operation = _operation_file(tmp_path)
    assert cli.main([str(operation), "--apply"]) == 2
    refused = json.loads(capsys.readouterr().err)
    assert "supplied together" in refused["message"]
    assert refused["production_activation_allowed"] is False
    assert refused["actionable_output_allowed"] is False


def test_apply_target_uses_only_dedicated_environment_and_independent_uuid() -> None:
    environ = {
        "DATABASE_URL": "mysql://root:secret@prod/prod",
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_URL": (
            "mysql+pymysql://authority_writer:secret@test-host/"
            "probiga_v2_evidence_test_authority"
        ),
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_SERVER_UUID": SERVER_UUID,
    }
    target = cli._resolve_apply_target("TEST", environ=environ)
    assert target.database_name == "probiga_v2_evidence_test_authority"
    assert target.expected_server_uuid == SERVER_UUID
    assert "DATABASE_URL" not in {
        target.url_environment_variable,
        target.uuid_environment_variable,
    }

    with pytest.raises(cli.AuthorityRegistrySafetyError, match="required"):
        cli._resolve_apply_target(
            "TEST",
            environ={"DATABASE_URL": environ["DATABASE_URL"]},
        )


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://root:secret@test-host/probiga_v2_evidence_test_authority",
        "mysql+pymysql://writer:@test-host/probiga_v2_evidence_test_authority",
        "mysql+pymysql://writer:secret@prod-host/probiga_v2_evidence_test_authority",
        "mysql+pymysql://writer:secret@production1/probiga_v2_evidence_test_authority",
        "mysql+pymysql://writer:secret@test-host/probiga_business_v2_evidence_test",
        "mysql+pymysql://writer:secret@test-host/probiga_v2_evidence_test?x=1",
        "postgresql://writer:secret@test-host/probiga_v2_evidence_test",
    ),
)
def test_apply_target_refuses_admin_production_business_query_and_non_mysql(url) -> None:
    with pytest.raises(cli.AuthorityRegistrySafetyError):
        cli._resolve_apply_target(
            "TEST",
            environ={
                "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_URL": url,
                "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_SERVER_UUID": SERVER_UUID,
            },
        )


def test_evidence_ledger_precheck_is_exactly_011_through_015() -> None:
    expected = cli._expected_evidence_ledger()
    assert expected == cli.FROZEN_EVIDENCE_LEDGER
    assert len(expected) == 5
    assert tuple(version.split("_")[1] for version, _checksum in expected) == (
        "011",
        "012",
        "013",
        "014",
        "015",
    )
    assert all(len(checksum) == 64 for _version, checksum in expected)


def test_evidence_ledger_precheck_rejects_code_drift(monkeypatch) -> None:
    migrations = [dict(item) for item in cli.MIGRATIONS]
    target = next(
        item for item in migrations if str(item["version"]).split("_")[1] == "014"
    )
    target["statements"] = (*tuple(target["statements"]), "SELECT 1")
    monkeypatch.setattr(cli, "MIGRATIONS", tuple(migrations))
    with pytest.raises(cli.AuthorityRegistrySafetyError, match="independently frozen"):
        cli._expected_evidence_ledger()


class _Transaction:
    def __init__(self) -> None:
        self.is_active = True
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True
        self.is_active = False

    def rollback(self) -> None:
        self.rolled_back = True
        self.is_active = False


class _Connection:
    def __init__(self) -> None:
        self.transaction = _Transaction()

    def begin(self) -> _Transaction:
        return self.transaction

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()
        self.disposed = False

    def connect(self) -> _Connection:
        return self.connection

    def dispose(self) -> None:
        self.disposed = True


def _target() -> cli.ApplyTarget:
    environ = {
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_URL": (
            "mysql+pymysql://authority_writer:secret@test-host/"
            "probiga_v2_evidence_test_authority"
        ),
        "V2_EVIDENCE_TEST_AUTHORITY_MYSQL_SERVER_UUID": SERVER_UUID,
    }
    return cli._resolve_apply_target("TEST", environ=environ)


def test_runtime_identity_accepts_mysql_8411_only_in_test_scope() -> None:
    target = _target()

    class Result:
        def mappings(self):
            return self

        def one(self):
            return {
                "server_version": "8.4.11",
                "version_comment": "MySQL Community Server (GPL)",
                "database_name": target.database_name,
                "server_uuid": target.expected_server_uuid,
            }

    class Connection:
        dialect = SimpleNamespace(name="mysql")

        def execute(self, _statement):
            return Result()

    identity = cli._runtime_identity(
        Connection(),  # type: ignore[arg-type]
        target,
    )

    assert identity.server_version == "8.4.11"


def test_apply_runs_identity_fence_ledger_schema_and_pre_post_audits_before_commit(
    tmp_path, monkeypatch
) -> None:
    plan = cli.parse_operation(str(_operation_file(tmp_path)))
    engine = _Engine()
    calls: list[str] = []
    audit = SimpleNamespace(
        audit_passed=True,
        production_activation_allowed=False,
        rows_reconstructed=0,
    )
    result = SimpleNamespace(
        status=SimpleNamespace(value="INSERTED"),
        database_owned_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(cli, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(
        cli,
        "_runtime_identity",
        lambda connection, target: (
            calls.append("identity"),
            cli.RuntimeIdentity(
                target.database_name, "5.7.38", SERVER_UUID, "MySQL Community Server"
            ),
        )[1],
    )
    monkeypatch.setattr(
        cli,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda connection: calls.append("fence"),
    )
    monkeypatch.setattr(
        cli,
        "_assert_exact_evidence_ledger",
        lambda connection: (calls.append("ledger"), ("011", "012", "013", "014", "015"))[1],
    )
    monkeypatch.setattr(
        cli,
        "_assert_schema_preflight",
        lambda connection: calls.append("schema"),
    )
    monkeypatch.setattr(
        cli,
        "_assert_authority_audit",
        lambda connection, phase: (calls.append(f"audit-{phase}"), audit)[1],
    )
    monkeypatch.setattr(
        cli,
        "_append_operation",
        lambda connection, observed: (calls.append("append"), result)[1],
    )

    output = cli._apply(plan, _target())
    assert calls == [
        "identity",
        "fence",
        "ledger",
        "schema",
        "audit-pre-write",
        "append",
        "audit-post-write",
    ]
    assert engine.connection.transaction.committed is True
    assert engine.connection.transaction.rolled_back is False
    assert engine.disposed is True
    assert output["production_activation_allowed"] is False
    assert output["actionable_output_allowed"] is False


def test_apply_failure_rolls_back_and_disposes(tmp_path, monkeypatch) -> None:
    plan = cli.parse_operation(str(_operation_file(tmp_path)))
    engine = _Engine()
    monkeypatch.setattr(cli, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(
        cli,
        "_runtime_identity",
        lambda connection, target: (_ for _ in ()).throw(
            cli.AuthorityRegistrySafetyError("identity refused")
        ),
    )
    with pytest.raises(cli.AuthorityRegistrySafetyError, match="identity refused"):
        cli._apply(plan, _target())
    assert engine.connection.transaction.rolled_back is True
    assert engine.connection.transaction.committed is False
    assert engine.disposed is True
