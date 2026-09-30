from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from tools.secondary_edge import ai_probe


NEW_IDS = ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222")


@pytest.fixture
def config(tmp_path):
    codex = tmp_path / "codex.exe"
    chrome = tmp_path / "chrome.exe"
    codex.touch()
    chrome.touch()
    return {"ai": {"codex_exe": str(codex), "chrome_exe": str(chrome),
                   "profile_dir": str(tmp_path / "private-ai" / "chrome-profile"),
                   "server_url": "https://example.invalid"}}


@pytest.fixture
def providers():
    calls = []

    class Codex:
        def __init__(self, timeout):
            calls.append(("codex_init", timeout, os.environ["CODEX_HOME"]))
            self.thread_ids = dict.fromkeys(("stock", "general"), next(iter(ai_probe.PRODUCTION_THREADS)))

        def ask(self, channel, question):
            assert self.thread_ids[channel] in NEW_IDS
            assert all(value not in ai_probe.PRODUCTION_THREADS for value in self.thread_ids.values())
            calls.append(("codex_ask", channel, self.thread_ids[channel], os.environ["CODEX_HOME"]))
            return question.split("Reply exactly ", 1)[1].rstrip(".")

    class DeepSeek:
        def __init__(self, **kwargs):
            calls.append(("deepseek_init", kwargs, os.environ["PROBIGA_DEEPSEEK_CHROME_EXE"]))

        def prepare(self):
            calls.append(("deepseek_prepare",))
            return True

        def ask(self, question):
            calls.append(("deepseek_ask",))
            return question.split("Reply exactly ", 1)[1].rstrip(".")

    return SimpleNamespace(CodexTaskProvider=Codex, DeepSeekWebProvider=DeepSeek, calls=calls)


@pytest.fixture
def cli(monkeypatch):
    calls = []
    ids = iter(NEW_IDS)
    monkeypatch.setattr(ai_probe.shutil, "which", lambda name: "git.exe")

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0] == "git.exe":
            (Path(command[-1]) / ".git").mkdir()
            return subprocess.CompletedProcess(command, 0, b"", b"")
        assert command[1] == "exec"
        assert "resume" not in command and "fork" not in command
        assert "--ignore-user-config" in command
        assert command[command.index("--sandbox") + 1] == "read-only"
        assert not any(value in ai_probe.PRODUCTION_THREADS for value in command)
        event = {"type": "thread.started", "thread_id": next(ids)}
        return subprocess.CompletedProcess(command, 0, json.dumps(event).encode(), b"")

    monkeypatch.setattr(ai_probe.subprocess, "run", run)
    return calls


@pytest.fixture
def api(monkeypatch):
    import httpx
    calls = []
    result = {"status": 200, "payload": {"status": "ok", "release_revision": {"deployment_mode": "production"}}}

    class Client:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            calls.append(("GET", url))
            return SimpleNamespace(status_code=result["status"], json=lambda: result["payload"])

        def post(self, *args, **kwargs):
            raise AssertionError("Production queue writes are forbidden")

    monkeypatch.setattr(httpx, "Client", Client)
    return result, calls


def test_success_creates_private_sessions_and_only_gets_health(config, providers, cli, api, monkeypatch):
    original_cwd = Path.cwd()
    monkeypatch.setenv("CODEX_HOME", str(original_cwd / "existing-production-home"))
    monkeypatch.setenv("PROBIGA_CODEX_STOCK_THREAD_ID", next(iter(ai_probe.PRODUCTION_THREADS)))
    monkeypatch.setenv("PROBIGA_CODEX_GENERAL_THREAD_ID", next(iter(ai_probe.PRODUCTION_THREADS)))
    before = dict(os.environ)
    report = ai_probe.run_probe(config, providers=providers)
    assert report["status"] == "pass"
    assert {name: value["status"] for name, value in report["checks"].items()} == {
        "codex": "pass", "deepseek": "pass", "api": "pass"}
    assert report["production_queue_accessed"] is False
    assert report["production_takeover_authorized"] is False
    assert report["history_migration"]["status"] == "blocked"
    assert report["history_migration"]["production_threads_modified"] is False
    assert [call[1] for call in providers.calls if call[0] == "codex_ask"] == ["stock", "general"]
    expected_home = str(Path(config["ai"]["profile_dir"]).parent / "codex-home")
    assert report["checks"]["codex"]["codex_home"] == expected_home
    assert all(call[1]["env"]["CODEX_HOME"] == expected_home for call in cli)
    assert api[1][-1] == ("GET", "https://example.invalid/api/health")
    assert api[1][0][1]["trust_env"] is False
    assert api[1][0][1]["follow_redirects"] is False
    assert dict(os.environ) == before
    assert Path.cwd() == original_cwd
    assert "answer_sha256" in report["checks"]["codex"]["channels"]["stock"]
    assert "PROBIGA_AI_PROBE_STOCK_" not in json.dumps(report)


def test_codex_cannot_fall_back_to_a_production_thread(config, providers, cli, api, monkeypatch):
    original = ai_probe.subprocess.run

    def run(command, **kwargs):
        if command[0] == "git.exe":
            return original(command, **kwargs)
        event = {"type": "thread.started", "thread_id": next(iter(ai_probe.PRODUCTION_THREADS))}
        return subprocess.CompletedProcess(command, 0, json.dumps(event).encode(), b"")

    monkeypatch.setattr(ai_probe.subprocess, "run", run)
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["codex"]["status"] == "fail"
    assert not any(call[0] == "codex_ask" for call in providers.calls)


def test_codex_failed_login_is_not_a_pass_and_private_output_is_not_reported(config, providers, cli, api, monkeypatch):
    original = ai_probe.subprocess.run
    before = dict(os.environ)

    def run(command, **kwargs):
        if command[0] == "git.exe":
            return original(command, **kwargs)
        return subprocess.CompletedProcess(command, 1, b"secret question", b"token=DO_NOT_REPORT")

    monkeypatch.setattr(ai_probe.subprocess, "run", run)
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["codex"]["status"] == "fail"
    assert not any(call[0] == "codex_ask" for call in providers.calls)
    assert "DO_NOT_REPORT" not in json.dumps(report)
    assert "secret question" not in json.dumps(report)
    assert dict(os.environ) == before


def test_codex_channels_must_have_distinct_new_sessions(config, providers, cli, api, monkeypatch):
    original = ai_probe.subprocess.run

    def run(command, **kwargs):
        if command[0] == "git.exe":
            return original(command, **kwargs)
        event = {"type": "thread.started", "thread_id": NEW_IDS[0]}
        return subprocess.CompletedProcess(command, 0, json.dumps(event).encode(), b"")

    monkeypatch.setattr(ai_probe.subprocess, "run", run)
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["codex"]["channels"]["stock"]["status"] == "pass"
    assert report["checks"]["codex"]["channels"]["general"]["status"] == "fail"
    assert len([call for call in providers.calls if call[0] == "codex_ask"]) == 1


def test_deepseek_unlogged_window_is_not_generation_success(config, providers, cli, api, monkeypatch):
    monkeypatch.setattr(providers.DeepSeekWebProvider, "prepare", lambda self: False)
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["deepseek"]["reason"] == "deepseek_manual_login_required"
    assert report["checks"]["deepseek"]["status"] == "fail"
    assert not any(call[0] == "deepseek_ask" for call in providers.calls)


def test_deepseek_ready_without_verified_answer_does_not_pass(config, providers, cli, api, monkeypatch):
    monkeypatch.setattr(providers.DeepSeekWebProvider, "ask", lambda self, question: "stale previous answer")
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["deepseek"]["status"] == "fail"
    assert report["checks"]["deepseek"]["reason"] == "deepseek_generation_not_verified"


@pytest.mark.parametrize("payload", ["<html>Login</html>", {"status": "ok"},
                                    {"status": "degraded", "release_revision": {"deployment_mode": "production"}},
                                    {"status": "ok", "release_revision": {"deployment_mode": "development"}}])
def test_arbitrary_200_is_not_production_health(config, api, payload):
    api[0]["payload"] = payload
    result = ai_probe.probe_api(config["ai"])
    assert result["status"] == "fail"
    assert result["http_status"] == 200
    assert result["reachable"] is True


def test_api_degraded_http_status_is_recorded(config, api):
    api[0]["status"] = 503
    result = ai_probe.probe_api(config["ai"])
    assert result["status"] == "fail"
    assert result["http_status"] == 503
    assert result["reachable"] is True


def test_api_url_credentials_are_rejected_without_request(config, api):
    config["ai"]["server_url"] = "https://user:secret@example.invalid"
    result = ai_probe.probe_api(config["ai"])
    assert result["status"] == "fail"
    assert not api[1]
    assert "secret" not in json.dumps(result)


def test_missing_git_fails_codex_without_starting_cli(config, providers, api, monkeypatch):
    monkeypatch.setattr(ai_probe.shutil, "which", lambda name: None)
    monkeypatch.setattr(ai_probe.subprocess, "run", lambda *args, **kwargs: pytest.fail("must not launch CLI"))
    report = ai_probe.run_probe(config, providers=providers)
    assert report["checks"]["codex"]["status"] == "fail"
    assert report["checks"]["deepseek"]["status"] == "pass"
    assert report["checks"]["api"]["status"] == "pass"


def test_production_home_is_not_accepted(config, monkeypatch):
    monkeypatch.setattr(ai_probe.Path, "home", lambda: Path(config["ai"]["profile_dir"]).parent.parent)
    config["ai"]["profile_dir"] = str(Path.home() / ".codex" / "profile")
    with pytest.raises(ValueError):
        ai_probe.isolation_paths(config["ai"])


def test_provider_import_does_not_require_or_persist_remote_host(monkeypatch):
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_HOST", raising=False)
    sentinel = object()

    def load(name):
        assert name == "tools.run_codex_web_bridge"
        assert os.environ["PROBIGA_REMOTE_SSH_HOST"] == "127.0.0.1"
        return sentinel

    monkeypatch.setattr(ai_probe.importlib, "import_module", load)
    assert ai_probe.load_providers() is sentinel
    assert "PROBIGA_REMOTE_SSH_HOST" not in os.environ


def test_main_writes_explicit_three_failures_on_invalid_config(tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text("not-json", encoding="utf-8")
    assert ai_probe.main(["--config", str(config)]) == 2
    report = json.loads((tmp_path / "ai-status.json").read_text(encoding="utf-8"))
    assert list(report["checks"]) == ["codex", "deepseek", "api"]
    assert all(item["status"] == "fail" for item in report["checks"].values())
    assert not (tmp_path / "ai-status.json.tmp").exists()
    assert "not-json" not in capsys.readouterr().out
