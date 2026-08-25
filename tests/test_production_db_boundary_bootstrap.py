from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "deploy" / "production_db_boundary_bootstrap.py"
SPEC = importlib.util.spec_from_file_location("production_db_boundary_bootstrap", PATH)
assert SPEC and SPEC.loader
bootstrap = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bootstrap
SPEC.loader.exec_module(bootstrap)


def option(user: str, password: str = "A" * 64) -> bytes:
    return (
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        f"user={user}\npassword={password}\n"
    ).encode()


@pytest.mark.parametrize(
    "user", ["probiga_trigger_admin", "probiga_migrator"]
)
def test_strict_option_contract_accepts_only_expected_shape(user: str) -> None:
    bootstrap._parse_option(option(user), user)
    with pytest.raises(bootstrap.BoundaryError):
        bootstrap._parse_option(option(user) + b"extra=true\n", user)
    with pytest.raises(bootstrap.BoundaryError):
        bootstrap._parse_option(option(user, "short"), user)
    with pytest.raises(bootstrap.BoundaryError):
        bootstrap._parse_option(option(user).replace(b"13306", b"3306"), user)


def test_env_update_is_unique_and_fail_closed() -> None:
    source = (
        b"A=1\nMYSQL_SSL_CA=/etc/probiga/mysql-combined-ca.pem\n"
        b"MYSQL_TLS_REQUIRED=true\n"
    )
    result = bootstrap._render_env(source)
    assert result.count(b"MYSQL_SSL_CA=") == 1
    assert b"MYSQL_SSL_CA=/etc/probiga/mysql84-ca.pem" in result
    with pytest.raises(bootstrap.BoundaryError):
        bootstrap._render_env(source + b"MYSQL_TLS_REQUIRED=true\n")


def test_cli_failure_never_emits_secret(monkeypatch, capsys) -> None:
    secret = "S" * 64
    monkeypatch.setattr(
        bootstrap,
        "run",
        lambda *_: (_ for _ in ()).throw(bootstrap.BoundaryError("option-policy")),
    )
    assert bootstrap.main(["prepare"]) == 2
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "option-policy" in output.err


def test_source_contains_atomic_failure_and_toctou_guards() -> None:
    source = PATH.read_text(encoding="utf-8")
    for marker in (
        "partial-install",
        "installed-stage-present",
        "stat.S_ISLNK",
        "st_nlink != 1",
        "after-options",
        "after-env",
        "BoundaryError(\"toctou\")",
        "os.replace(temporary, path)",
        "before-claim-rename",
        "os.fchmod(descriptor, mode)",
        "transaction.committed",
    ):
        assert marker in source


@pytest.fixture
def linux_boundary(tmp_path: Path, monkeypatch):
    if os.name != "posix":
        pytest.skip("POSIX ownership and no-follow integration test")
    uid, gid = os.geteuid(), os.getegid()
    config = tmp_path / "etc" / "probiga"
    stage = tmp_path / "home" / "probiga-deploy" / ".probiga-db-boundary-stage"
    app = tmp_path / "opt" / "ProBigA"
    config.mkdir(parents=True, mode=0o755)
    stage.mkdir(parents=True, mode=0o700)
    app.mkdir(parents=True, mode=0o755)
    os.chmod(config, 0o755)
    os.chmod(stage, 0o700)
    os.chmod(app, 0o755)
    admin = config / "mysql-trigger-admin.ini"
    migrator = config / "mysql-migrator.ini"
    env = app / ".env"
    env.write_bytes(
        b"A=1\nMYSQL_SSL_CA=/etc/probiga/mysql-combined-ca.pem\n"
        b"MYSQL_TLS_REQUIRED=true\n"
    )
    os.chmod(env, 0o640)
    for name, user in bootstrap.FILES.items():
        candidate = stage / name
        candidate.write_bytes(option(user))
        os.chmod(candidate, 0o600)
    monkeypatch.setattr(bootstrap, "ROOT_UID", uid)
    monkeypatch.setattr(bootstrap, "ROOT_GID", gid)
    monkeypatch.setattr(bootstrap, "STAGE", stage)
    monkeypatch.setattr(bootstrap, "CONFIG_DIR", config)
    monkeypatch.setattr(bootstrap, "ADMIN", admin)
    monkeypatch.setattr(bootstrap, "MIGRATOR", migrator)
    transaction = config / ".database-boundary-bootstrap.transaction"
    monkeypatch.setattr(bootstrap, "TRANSACTION", transaction)
    monkeypatch.setattr(
        bootstrap,
        "TRANSACTION_BUILD",
        config / ".database-boundary-bootstrap.transaction.build",
    )
    monkeypatch.setattr(
        bootstrap,
        "TRANSACTION_COMMITTED",
        config / ".database-boundary-bootstrap.transaction.committed",
    )
    monkeypatch.setattr(
        bootstrap,
        "TRANSACTION_ROLLED_BACK",
        config / ".database-boundary-bootstrap.transaction.rolled-back",
    )
    monkeypatch.setattr(bootstrap, "CA", config / "mysql84-ca.pem")
    monkeypatch.setattr(bootstrap, "ENV", env)
    monkeypatch.setattr(bootstrap, "APP_ROOT", app)
    monkeypatch.setattr(
        bootstrap, "ENV_TEMP", app / ".env.database-boundary-bootstrap"
    )
    monkeypatch.setattr(bootstrap, "_validate_ca", lambda: b"test-ca")
    monkeypatch.setattr(bootstrap, "_validate_stage_parent", lambda _: None)
    import grp
    import pwd

    monkeypatch.setattr(
        pwd, "getpwnam", lambda _: SimpleNamespace(pw_uid=uid, pw_gid=gid)
    )
    monkeypatch.setattr(grp, "getgrnam", lambda _: SimpleNamespace(gr_gid=gid))
    return SimpleNamespace(
        config=config,
        stage=stage,
        app=app,
        env=env,
        admin=admin,
        migrator=migrator,
        transaction=transaction,
    )


def test_posix_success_and_repeat_are_idempotent(linux_boundary) -> None:
    first = bootstrap.run()
    assert first["mode"] == "prepared"
    assert not linux_boundary.stage.exists()
    assert linux_boundary.transaction.is_dir()
    assert linux_boundary.admin.stat().st_mode & 0o777 == 0o600
    assert linux_boundary.migrator.stat().st_mode & 0o777 == 0o600
    assert b"mysql84-ca.pem" in linux_boundary.env.read_bytes()
    committed = bootstrap.run("commit")
    assert committed["mode"] == "committed"
    assert not linux_boundary.transaction.exists()
    second = bootstrap.run()
    assert second["mode"] == "already-installed"
    assert second["hashes"] == committed["hashes"]


def test_posix_partial_and_symlink_fail_without_change(linux_boundary) -> None:
    original = linux_boundary.env.read_bytes()
    linux_boundary.admin.write_bytes(option("probiga_trigger_admin"))
    os.chmod(linux_boundary.admin, 0o600)
    with pytest.raises(bootstrap.BoundaryError, match="partial-install"):
        bootstrap.run()
    assert linux_boundary.env.read_bytes() == original
    linux_boundary.admin.unlink()
    real_stage = linux_boundary.stage.with_name("real-stage")
    linux_boundary.stage.rename(real_stage)
    linux_boundary.stage.symlink_to(real_stage, target_is_directory=True)
    with pytest.raises(bootstrap.BoundaryError):
        bootstrap.run()
    assert linux_boundary.env.read_bytes() == original
    assert not linux_boundary.admin.exists()
    assert not linux_boundary.migrator.exists()


def test_posix_fault_rolls_back_env_metadata_options_and_stage(linux_boundary) -> None:
    original = linux_boundary.env.read_bytes()
    original_mode = linux_boundary.env.stat().st_mode & 0o777

    def fault(point: str) -> None:
        if point == "after-env":
            raise bootstrap.BoundaryError("injected")

    with pytest.raises(bootstrap.BoundaryError, match="injected"):
        bootstrap.run(fault_hook=fault)
    assert linux_boundary.env.read_bytes() == original
    assert linux_boundary.env.stat().st_mode & 0o777 == original_mode
    assert linux_boundary.stage.is_dir()
    assert not linux_boundary.admin.exists()
    assert not linux_boundary.migrator.exists()


def test_posix_explicit_rollback_restores_stage_and_live_metadata(linux_boundary) -> None:
    original = linux_boundary.env.read_bytes()
    prepared = bootstrap.run("prepare")
    assert prepared["mode"] == "prepared"
    repeated = bootstrap.run("prepare")
    assert repeated["mode"] == "prepared"
    rolled_back = bootstrap.run("rollback")
    assert rolled_back["mode"] == "rolled-back"
    assert linux_boundary.env.read_bytes() == original
    assert linux_boundary.stage.is_dir()
    assert not linux_boundary.transaction.exists()
    assert not linux_boundary.admin.exists()
    assert not linux_boundary.migrator.exists()


def test_posix_stage_swap_is_detected_before_mutation(linux_boundary) -> None:
    original = linux_boundary.env.read_bytes()

    def swap(point: str) -> None:
        if point == "after-preflight":
            candidate = linux_boundary.stage / "mysql-migrator.ini"
            candidate.write_bytes(option("probiga_migrator", "B" * 64))
            os.chmod(candidate, 0o600)

    with pytest.raises(bootstrap.BoundaryError, match="toctou"):
        bootstrap.run(fault_hook=swap)
    assert linux_boundary.env.read_bytes() == original
    assert not linux_boundary.admin.exists()
    assert not linux_boundary.migrator.exists()
    assert linux_boundary.stage.is_dir()


def test_posix_claim_symlink_swap_never_chmods_target(linux_boundary) -> None:
    target = linux_boundary.app.parent / "sentinel"
    target.mkdir(mode=0o755)
    os.chmod(target, 0o755)
    displaced = linux_boundary.stage.with_name("displaced-stage")

    def swap(point: str) -> None:
        if point == "before-claim-rename":
            linux_boundary.stage.rename(displaced)
            linux_boundary.stage.symlink_to(target, target_is_directory=True)

    with pytest.raises(bootstrap.BoundaryError, match="rollback-incomplete"):
        bootstrap.run("prepare", fault_hook=swap)
    assert target.stat().st_mode & 0o777 == 0o755
    assert not linux_boundary.admin.exists()
    assert not linux_boundary.migrator.exists()
