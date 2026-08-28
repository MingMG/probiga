from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from tools import migrate_qmt_local_history_provenance as migration


class _Engine:
    def __init__(self, database="probiga_qmt_history"):
        self.disposed = False
        self.url = type("Url", (), {"database": database})()

    def dispose(self):
        self.disposed = True


def test_default_cli_check_is_read_only_and_blocks_missing_schema(
    monkeypatch,
    capsys,
):
    engine = _Engine()
    calls = []
    monkeypatch.setattr(migration, "load_project_env", lambda: calls.append("env"))
    monkeypatch.setattr(
        migration,
        "get_local_history_engine",
        lambda: engine,
    )

    def missing(_engine, *, database=None):
        calls.append("validate")
        assert database == "probiga_qmt_history"
        raise migration.LocalHistoryProvenanceSchemaError(
            "pre_close_origin missing"
        )

    monkeypatch.setattr(
        migration,
        "validate_local_history_provenance_schema",
        missing,
    )
    monkeypatch.setattr(
        migration,
        "migrate_local_history_provenance_schema",
        lambda *_args, **_kwargs: calls.append("migrate"),
    )

    assert migration.main([]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["native_qmt_inferred"] is False
    assert payload["legacy_rows_default_to"] == "UNVERIFIED_LEGACY"
    assert calls == ["env", "validate"]
    assert engine.disposed is True


def test_apply_cli_invokes_only_explicit_migration(monkeypatch, capsys):
    engine = _Engine()
    calls = []
    monkeypatch.setattr(migration, "load_project_env", lambda: None)
    monkeypatch.setattr(
        migration,
        "get_local_history_engine",
        lambda: engine,
    )

    def apply_schema(actual_engine, *, apply=False, database=None):
        calls.append((actual_engine, apply, database))
        return {
            "status": "applied",
            "applied": True,
            "ready": True,
            "legacy_rows_default_to": "UNVERIFIED_LEGACY",
        }

    monkeypatch.setattr(
        migration,
        "migrate_local_history_provenance_schema",
        apply_schema,
    )

    assert migration.main(["--apply"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "applied"
    assert payload["native_qmt_inferred"] is False
    assert calls == [(engine, True, "probiga_qmt_history")]
    assert engine.disposed is True


def test_primary_route_check_uses_read_only_qualified_connection(
    monkeypatch,
    capsys,
):
    history_engine = _Engine()
    primary_engine = _Engine(database="probiga")
    calls = []
    monkeypatch.setattr(migration, "load_project_env", lambda: None)
    monkeypatch.setattr(
        migration,
        "get_local_history_engine",
        lambda: history_engine,
    )
    monkeypatch.setattr(
        migration,
        "create_batch_engine",
        lambda *, future=False: primary_engine,
    )

    def validate(engine, *, database=None):
        calls.append((engine, database))
        return {
            "ready": True,
            "qualified_table": (
                "`probiga_qmt_history`.`qmt_local_stock_kline`"
            ),
        }

    monkeypatch.setattr(
        migration,
        "validate_local_history_provenance_schema",
        validate,
    )

    assert migration.main(["--check-via-primary"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert calls == [(primary_engine, "probiga_qmt_history")]
    assert primary_engine.disposed is True
    assert history_engine.disposed is True


def _private_acl_snapshot():
    current_sid = "S-1-5-21-1000"
    return {
        "owner_sid": current_sid,
        "current_user_sid": current_sid,
        "protected": True,
        "rules": [
            {
                "sid": current_sid,
                "access_type": "Allow",
                "inherited": False,
                "rights": 0x1F01FF,
            },
            {
                "sid": migration._WINDOWS_ADMINISTRATORS_SID,
                "access_type": "Allow",
                "inherited": False,
                "rights": 0x1F01FF,
            },
            {
                "sid": migration._WINDOWS_SYSTEM_SID,
                "access_type": "Allow",
                "inherited": False,
                "rights": 0x1F01FF,
            },
        ],
    }


def _write_option_file(path, *, secret="A" * 64, extra="", host="127.0.0.1"):
    path.write_text(
        "[client]\n"
        "protocol=TCP\n"
        f"host={host}\n"
        "port=3306\n"
        "user=probiga_runtime\n"
        f"password={secret}\n"
        f"{extra}",
        encoding="utf-8",
    )


def test_windows_option_engine_and_driver_arguments_never_contain_secret(
    tmp_path,
    monkeypatch,
):
    secret = "S" * 64
    option_file = tmp_path / "runtime.ini"
    _write_option_file(option_file, secret=secret)
    monkeypatch.setattr(
        migration,
        "_protected_windows_option_file",
        lambda path: path.resolve(),
    )
    observed = {}

    def fake_connect(**kwargs):
        observed.update(kwargs)
        return object()

    monkeypatch.setattr(migration.pymysql, "connect", fake_connect)

    engine = migration._create_windows_local_history_engine(option_file)
    try:
        assert repr(engine.url) == "mysql+pymysql:///probiga_qmt_history"
        assert engine.url.username is None
        assert engine.url.password is None
        assert secret not in repr(engine.url)
        connection = migration._connect_from_windows_option_file(option_file)
    finally:
        engine.dispose()

    assert connection is not None
    assert "password" not in observed
    assert "passwd" not in observed
    assert "user" not in observed
    assert "host" not in observed
    assert "port" not in observed
    assert observed["read_default_file"] == str(option_file.resolve())
    assert secret not in repr(observed)


@pytest.mark.parametrize(
    ("snapshot_update", "rule_update"),
    [
        ({"protected": False}, None),
        ({"owner_sid": "S-1-5-21-OTHER"}, None),
        ({}, {"inherited": True}),
        ({}, {"sid": "S-1-5-11"}),
        ({}, {"access_type": "Deny"}),
        ({}, {"rights": 0}),
    ],
)
def test_windows_acl_boundary_rejects_non_private_rules(
    snapshot_update,
    rule_update,
):
    snapshot = _private_acl_snapshot()
    snapshot.update(snapshot_update)
    if rule_update:
        snapshot["rules"][0].update(rule_update)

    with pytest.raises(
        migration.WindowsLocalHistoryBoundaryError,
        match="DACL",
    ):
        migration._validate_windows_acl_snapshot(snapshot)


@pytest.mark.parametrize(
    ("host", "extra", "secret"),
    [
        ("db.example.invalid", "", "A" * 64),
        ("127.0.0.1", "database=probiga\n", "A" * 64),
        ("127.0.0.1", "", "short-secret"),
    ],
)
def test_windows_option_file_rejects_remote_extra_or_weak_secret(
    tmp_path,
    host,
    extra,
    secret,
):
    option_file = tmp_path / "runtime.ini"
    _write_option_file(
        option_file,
        host=host,
        extra=extra,
        secret=secret,
    )

    with pytest.raises(
        migration.WindowsLocalHistoryBoundaryError,
        match="shape or target differs",
    ) as captured:
        migration._validate_windows_option_file_shape(option_file)

    assert secret not in str(captured.value)


class _BoundaryResult:
    def __init__(self, mapping=None, row=None):
        self._mapping = mapping
        self._row = row

    def mappings(self):
        return self

    def one(self):
        return self._mapping if self._mapping is not None else self._row


class _BoundaryConnection:
    def __init__(self, state, tls_cipher="TLS_AES_256_GCM_SHA384"):
        self.state = state
        self.tls_cipher = tls_cipher

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement):
        if str(statement).startswith("SHOW SESSION STATUS"):
            return _BoundaryResult(row=("Ssl_cipher", self.tls_cipher))
        return _BoundaryResult(mapping=dict(self.state))


class _BoundaryEngine:
    def __init__(self, state, tls_cipher="TLS_AES_256_GCM_SHA384"):
        self.state = state
        self.tls_cipher = tls_cipher

    def connect(self):
        return _BoundaryConnection(self.state, self.tls_cipher)


def _valid_boundary_state():
    return {
        "mysql_version": "8.4.11",
        "version_comment": "MySQL Community Server - GPL",
        "database_name": "probiga_qmt_history",
        "authenticated_user": "probiga_runtime@127.0.0.1",
        "server_uuid": "f40c3202-9260-11f1-86ae-74d4dd7f8500",
        "server_hostname": "WIN-20260322RGF",
        "server_port": 3306,
    }


def test_windows_mysql_boundary_requires_exact_local_84_identity_and_tls(
    monkeypatch,
):
    monkeypatch.setattr(
        migration.socket,
        "gethostname",
        lambda: "WIN-20260322RGF",
    )
    result = migration._validate_windows_local_mysql84_boundary(
        _BoundaryEngine(_valid_boundary_state())
    )
    assert result == {
        "ready": True,
        "mysql_version": "8.4.11",
        "database": "probiga_qmt_history",
        "server_uuid": "f40c3202-9260-11f1-86ae-74d4dd7f8500",
        "server_hostname": "WIN-20260322RGF",
        "server_port": 3306,
        "tls": True,
    }

    wrong_server = _valid_boundary_state()
    wrong_server["server_uuid"] = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(
        migration.WindowsLocalHistoryBoundaryError,
        match="identity or TLS",
    ):
        migration._validate_windows_local_mysql84_boundary(
            _BoundaryEngine(wrong_server)
        )
    with pytest.raises(
        migration.WindowsLocalHistoryBoundaryError,
        match="identity or TLS",
    ):
        migration._validate_windows_local_mysql84_boundary(
            _BoundaryEngine(_valid_boundary_state(), tls_cipher="")
        )


def test_windows_cli_route_skips_env_and_redacts_option_secret(
    monkeypatch,
    capsys,
):
    secret = "Z" * 64
    engine = _Engine()
    engine.url = make_url("mysql+pymysql:///probiga_qmt_history")
    events = []
    monkeypatch.setattr(
        migration,
        "load_project_env",
        lambda: (_ for _ in ()).throw(AssertionError("env route used")),
    )
    monkeypatch.setattr(
        migration,
        "_create_windows_local_history_engine",
        lambda: events.append("engine") or engine,
    )
    monkeypatch.setattr(
        migration,
        "_validate_windows_local_mysql84_boundary",
        lambda actual: events.append(("boundary", actual)) or {"ready": True},
    )
    monkeypatch.setattr(
        migration,
        "validate_local_history_provenance_schema",
        lambda actual, *, database=None: events.append(
            ("schema", actual, database)
        )
        or {"ready": True},
    )

    assert migration.main(["--windows-local-option-file"]) == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["status"] == "ok"
    assert secret not in output
    assert events == [
        "engine",
        ("boundary", engine),
        ("schema", engine, "probiga_qmt_history"),
    ]
    assert engine.disposed is True


def test_windows_runtime_identity_rejects_apply_before_database_access(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        migration,
        "_create_windows_local_history_engine",
        lambda: (_ for _ in ()).throw(
            AssertionError("runtime database access must not start")
        ),
    )
    monkeypatch.setattr(
        migration,
        "load_project_env",
        lambda: (_ for _ in ()).throw(
            AssertionError("privileged environment must not load")
        ),
    )

    with pytest.raises(SystemExit) as failure:
        migration.main(["--apply", "--windows-local-option-file"])

    assert failure.value.code == 2
    error = capsys.readouterr().err
    assert "dedicated privileged database connection" in error
    assert "read-only runtime identity" in error


def test_windows_cli_boundary_failure_is_generic_and_disposes(
    monkeypatch,
    capsys,
):
    engine = _Engine()
    engine.url = make_url("mysql+pymysql:///probiga_qmt_history")
    monkeypatch.setattr(
        migration,
        "_create_windows_local_history_engine",
        lambda: engine,
    )
    monkeypatch.setattr(
        migration,
        "_validate_windows_local_mysql84_boundary",
        lambda _engine: (_ for _ in ()).throw(
            migration.WindowsLocalHistoryBoundaryError(
                "fixed Windows MySQL 8.4 identity or TLS boundary differs"
            )
        ),
    )

    assert migration.main(["--windows-local-option-file"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["reason"] == (
        "fixed Windows MySQL 8.4 identity or TLS boundary differs"
    )
    assert payload["native_qmt_inferred"] is False
    assert engine.disposed is True
