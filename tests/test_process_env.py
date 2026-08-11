# -*- coding: utf-8 -*-
import os
from types import SimpleNamespace

from sqlalchemy.engine import make_url

from server.common import adata_release
from server.common.process_env import build_child_env, child_process_timeout, mask_url, temporary_env


def test_build_child_env_prioritizes_verified_adata_and_filters_mutable_checkout(
    monkeypatch,
    tmp_path,
):
    root = tmp_path / "repo"
    mutable_adata = root / "adata"
    mutable_adata.mkdir(parents=True)
    verified_source = (tmp_path / "sealed-adata").resolve()
    package = verified_source / "adata"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    sealed = adata_release.seal_adata_release_source(
        verified_source,
        "a" * 40,
    )
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv(adata_release.ADATA_SOURCE_ENV, str(verified_source))
    monkeypatch.setenv(adata_release.ADATA_GIT_SHA_ENV, "a" * 40)
    monkeypatch.setenv(
        adata_release.ADATA_TREE_SHA_ENV,
        str(sealed["tree_sha256"]),
    )
    monkeypatch.setattr(adata_release.os, "access", lambda *_args: False)
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join((str(mutable_adata), str(root), "existing")),
    )

    env = build_child_env(
        root,
        extra_python_paths=[mutable_adata, root],
    )

    parts = env["PYTHONPATH"].split(os.pathsep)
    assert parts[:2] == [str(verified_source), str(root)]
    assert str(mutable_adata) not in parts
    assert "existing" in parts
    assert parts.count(str(root)) == 1


def test_build_child_env_development_fallback_prioritizes_bundled_adata(
    monkeypatch,
    tmp_path,
):
    root = tmp_path / "repo"
    bundled_adata = root / "adata"
    bundled_adata.mkdir(parents=True)
    monkeypatch.delenv("PYTHONPATH", raising=False)

    env = build_child_env(root)

    parts = env["PYTHONPATH"].split(os.pathsep)
    assert parts[:2] == [str(bundled_adata.resolve()), str(root)]


def test_build_child_env_injects_engine_url_and_can_preserve_existing_mysql(monkeypatch, tmp_path):
    root = tmp_path / "repo"
    engine = SimpleNamespace(url=make_url("mysql+pymysql://u:p@localhost:3306/probiga"))
    monkeypatch.setenv("MYSQL_URL", "mysql://existing")

    preserved = build_child_env(root, engine=engine, override_mysql_url=False)
    overridden = build_child_env(root, engine=engine)

    assert preserved["MYSQL_URL"] == "mysql://existing"
    assert overridden["MYSQL_URL"] == "mysql+pymysql://u:p@localhost:3306/probiga"


def test_build_child_env_forces_utf8_child_output(monkeypatch, tmp_path):
    monkeypatch.delenv("PYTHONIOENCODING", raising=False)
    monkeypatch.delenv("PYTHONUTF8", raising=False)

    env = build_child_env(tmp_path / "repo")

    assert env["PYTHONIOENCODING"] == "utf-8"
    assert env["PYTHONUTF8"] == "1"


def test_build_child_env_strips_dead_local_proxy(monkeypatch, tmp_path):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("HTTPS_PROXY", "http://localhost:7890")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.example:8080")
    monkeypatch.delenv("PROBIGA_KEEP_PROXY_ENV", raising=False)

    env = build_child_env(tmp_path / "repo")

    assert "HTTP_PROXY" not in env
    assert "HTTPS_PROXY" not in env
    assert env["ALL_PROXY"] == "http://proxy.example:8080"


def test_build_child_env_can_keep_proxy_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("PROBIGA_KEEP_PROXY_ENV", "1")

    env = build_child_env(tmp_path / "repo")

    assert env["HTTP_PROXY"] == "http://127.0.0.1:7890"


def test_mask_url_hides_password():
    assert mask_url("mysql+pymysql://user:secret@localhost:3306/db") == (
        "mysql+pymysql://user:***@localhost:3306/db"
    )


def test_child_process_timeout_uses_positive_env_value(monkeypatch):
    monkeypatch.setenv("PROBIGA_TEST_TIMEOUT", "12")

    assert child_process_timeout(30, env_name="PROBIGA_TEST_TIMEOUT") == 12


def test_child_process_timeout_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("PROBIGA_TEST_TIMEOUT", "bad")

    assert child_process_timeout(30, env_name="PROBIGA_TEST_TIMEOUT") == 30


def test_temporary_env_sets_and_restores(monkeypatch):
    monkeypatch.delenv("PROBIGA_TEST_TEMP", raising=False)

    with temporary_env({"PROBIGA_TEST_TEMP": "inside"}):
        assert os.environ["PROBIGA_TEST_TEMP"] == "inside"

    assert "PROBIGA_TEST_TEMP" not in os.environ


def test_temporary_env_can_preserve_existing_values(monkeypatch):
    monkeypatch.setenv("PROBIGA_TEST_EXISTING", "original")
    monkeypatch.delenv("PROBIGA_TEST_NEW", raising=False)

    with temporary_env(
        {
            "PROBIGA_TEST_EXISTING": "changed",
            "PROBIGA_TEST_NEW": "created",
        },
        overwrite=False,
    ):
        assert os.environ["PROBIGA_TEST_EXISTING"] == "original"
        assert os.environ["PROBIGA_TEST_NEW"] == "created"

    assert os.environ["PROBIGA_TEST_EXISTING"] == "original"
    assert "PROBIGA_TEST_NEW" not in os.environ
