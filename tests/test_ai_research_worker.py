from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from tools import run_codex_web_bridge as bridge


def job():
    payload = {"request_id": "research-1", "stage": "review", "rules_sha256": "a" * 64,
               "deadline_at": (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(),
               "messages": [{"role": "system", "content": "独立复核，只依据冻结事实。"},
                            {"role": "user", "content": "事实：指数下跌；请判断。"}],
               "response_schema": {"type": "object", "properties": {"verdict": {"type": "string"}},
                                   "required": ["verdict"], "additionalProperties": False}}
    payload["input_sha256"] = bridge._sha256(bridge._research_json({key: value for key, value in payload.items()
                                    if key not in {"request_id", "deadline_at", "input_sha256"}}))
    return {"request_id": "research-1", "worker_id": "worker", "channel": "research",
            "claim_generation": 1, "status": "processing", "payload": payload}


class Api:
    def __init__(self, mode="ok"):
        self.mode = mode
        self.renewals = 0
        self.posts = []
        self.gets = 0
        self.result = {"request_id": "research-1", "status": "processing", "worker_id": "worker", "claim_generation": 1}

    def renew_research(self, request_id, worker_id, generation):
        self.renewals += 1
        return {"request_id": request_id, "worker_id": worker_id, "claim_generation": generation, "status": "processing"}

    def complete_research(self, request_id, payload):
        self.posts.append(deepcopy(payload))
        result = {"request_id": request_id, "status": payload["status"], "source": "codex_gpt",
                  "raw_sha256": bridge._sha256(payload.get("raw_response", "").encode("utf-8"))}
        if self.mode == "lost_reply" and len(self.posts) == 1:
            self.result = result
            raise TimeoutError("reply lost after commit")
        if self.mode == "lost_request" and len(self.posts) == 1:
            raise TimeoutError("request not committed")
        if self.mode == "offline":
            raise TimeoutError("completion transport down")
        self.result = result
        return result

    def research_status(self, *_args):
        self.gets += 1
        return self.result


class Provider:
    def __init__(self):
        self.calls = 0

    def ask(self, payload, directory, lease_lost):
        self.calls += 1
        raw = '{"verdict":"通过"}\r\n'
        return {"raw_response": raw, "provider_receipt": {"source": "codex_gpt", "request_id": payload["request_id"],
                "input_sha256": payload["input_sha256"], "rules_sha256": payload["rules_sha256"],
                "raw_sha256": hashlib.sha256(raw.encode()).hexdigest()}}


@pytest.mark.parametrize("mode,posts", [("ok", 1), ("lost_reply", 1), ("lost_request", 2)])
def test_completion_uncertainty_never_repeats_the_model(mode, posts, tmp_path):
    api, provider = Api(mode), Provider()
    result = bridge.process_research_job(api, provider, job=job(), worker_id="worker", state_root=tmp_path)
    assert result["status"] == "completed"
    assert provider.calls == 1
    assert len(api.posts) == posts
    if posts == 2:
        assert api.posts[0] == api.posts[1]
    receipt = json.loads((tmp_path / "research-1" / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "COMPLETE"
    assert receipt["completion"]["raw_response"].endswith("\r\n")


def test_pending_completion_checkpoint_recovers_without_another_model_call(tmp_path):
    api, provider = Api("offline"), Provider()
    claim = job()
    with pytest.raises(TimeoutError):
        bridge.process_research_job(api, provider, job=claim, worker_id="worker", state_root=tmp_path)
    assert provider.calls == 1
    receipt = json.loads((tmp_path / "research-1" / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "MODEL_COMPLETE"
    api.mode = "ok"
    result = bridge.process_research_job(api, provider, job=claim, worker_id="worker", state_root=tmp_path)
    assert result["status"] == "completed"
    assert provider.calls == 1


def test_ambiguous_interrupted_model_checkpoint_fails_without_regeneration(tmp_path):
    claim = job()
    directory = tmp_path / claim["request_id"]
    bridge._research_checkpoint(directory, {"request_id": claim["request_id"], "worker_id": "worker", "claim_generation": 1,
                                          "input_sha256": claim["payload"]["input_sha256"], "status": "RUNNING"})
    provider = Provider()
    with pytest.raises(bridge.ProviderUnavailable, match="interrupted_model_outcome_unknown"):
        bridge.process_research_job(Api(), provider, job=claim, worker_id="worker", state_root=tmp_path)
    assert provider.calls == 0


@pytest.mark.parametrize("mutation", ["question", "hash", "rules", "generation", "traversal", "deadline"])
def test_untyped_changed_or_expired_claims_never_reach_a_model(mutation, tmp_path):
    claim = job()
    if mutation == "question":
        claim["channel"] = "stock"
    elif mutation == "hash":
        claim["payload"]["messages"][0]["content"] = "changed"
    elif mutation == "rules":
        claim["payload"]["rules_sha256"] = "not-a-hash"
    elif mutation == "generation":
        claim["claim_generation"] = 0
    elif mutation == "traversal":
        claim["request_id"] = claim["payload"]["request_id"] = "../escape"
    elif mutation == "deadline":
        claim["payload"]["deadline_at"] = "2000-01-01T00:00:00+00:00"
    provider = Provider()
    with pytest.raises(bridge.ProviderUnavailable):
        bridge.process_research_job(Api(), provider, job=claim, worker_id="worker", state_root=tmp_path)
    assert provider.calls == 0


@pytest.fixture
def offline_cli(tmp_path, monkeypatch):
    """A real local subprocess exercises stdin, output, deadlines and killing."""
    script = tmp_path / "offline_codex.py"
    script.write_text('''import json,sys,time
from pathlib import Path
args=sys.argv[1:]
prompt=json.loads(sys.stdin.buffer.read().decode("utf-8"))
if prompt["messages"][-1]["content"]=="HANG": time.sleep(30)
schema=json.loads(Path(args[args.index("--output-schema")+1]).read_bytes())
assert schema["type"]=="object"
raw='{"verdict":"通过"}\\r\\n'.encode("utf-8")
Path(args[args.index("-o")+1]).write_bytes(raw)
sys.stdout.buffer.write((json.dumps({"type":"thread.started","thread_id":"isolated-session"})+"\\n").encode())
''', encoding="utf-8")
    original = subprocess.Popen
    launches = []

    def launch(command, **kwargs):
        launches.append((command, kwargs))
        child = original([sys.executable, str(script), *command[1:]], **kwargs)
        launches[-1] = (command, kwargs, child)
        return child

    monkeypatch.setattr(bridge.subprocess, "Popen", launch)
    monkeypatch.setattr(bridge.CodexResearchProvider, "version", lambda self: "codex-test")
    return launches


def test_real_subprocess_is_ephemeral_tool_disabled_utf8_and_has_exact_raw_receipt(offline_cli, tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-pass")
    monkeypatch.setenv("OPENAI_BASE_URL", "must-not-pass")
    monkeypatch.setenv("CODEX_API_KEY", "must-not-pass")
    monkeypatch.chdir(tmp_path)
    directory = Path("process")
    directory.mkdir()
    result = bridge.CodexResearchProvider(5, executable=Path("codex.exe")).ask(job()["payload"], directory, threading.Event())
    command, options, child = offline_cli[0]
    assert "--ephemeral" in command and "resume" not in command
    assert "--ignore-user-config" in command
    assert command[-1] == "-"
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert all(name not in options["env"] for name in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "CODEX_API_KEY"))
    assert "shell_tool" in command and "apps" in command and "plugins" in command
    assert command.count("-c") == 2
    assert 'model_provider="probiga_openai_https"' in command
    assert any('supports_websockets=false,request_max_retries=0,stream_max_retries=0' in item for item in command)
    assert not any("responses_websockets" in item for item in command)
    assert "-m" not in command and "--model" not in command
    assert child.poll() == 0
    assert result["raw_response"] == '{"verdict":"通过"}\r\n'
    assert result["provider_receipt"]["raw_sha256"] == hashlib.sha256((directory / "answer.json").read_bytes()).hexdigest()
    assert result["provider_receipt"]["session_id"] == "isolated-session"
    assert result["provider_receipt"]["model"] == "UNKNOWN"
    assert result["provider_receipt"]["cli_version"] == "codex-test"
    assert result["provider_receipt"]["base_url"] == "https://chatgpt.com/backend-api/codex"
    assert result["provider_receipt"]["network_transport"] == "https_sse"
    assert result["provider_receipt"]["request_max_retries"] == 0
    state = json.loads((directory / "process" / "receipt.json").read_text(encoding="utf-8"))
    assert state["pid"] == child.pid
    assert state["status"] == "TERMINAL"
    assert state["input_sha256"] == job()["payload"]["input_sha256"]
    assert state["worker_source_sha256"] == bridge.WORKER_SOURCE_SHA256


def test_subprocess_deadline_kills_and_confirms_the_exact_child_terminal(offline_cli, tmp_path):
    directory = tmp_path / "process"
    directory.mkdir()
    payload = job()["payload"]
    payload["messages"][-1]["content"] = "HANG"
    started = time.monotonic()
    with pytest.raises(bridge.ProviderUnavailable, match="deadline_exceeded"):
        bridge.CodexResearchProvider(1, executable=Path("codex.exe")).ask(payload, directory, threading.Event())
    assert time.monotonic() - started < 7
    assert offline_cli[0][2].poll() is not None
    state = json.loads((directory / "process" / "receipt.json").read_text(encoding="utf-8"))
    assert state["status"] == "TERMINAL"
    assert state["pid"] == offline_cli[0][2].pid


def test_lease_loss_terminates_subprocess_without_fallback(offline_cli, tmp_path):
    directory = tmp_path / "process"
    directory.mkdir()
    payload = job()["payload"]
    payload["messages"][-1]["content"] = "HANG"
    lost = threading.Event()
    timer = threading.Timer(0.25, lost.set)
    timer.start()
    try:
        with pytest.raises(bridge.ProviderUnavailable, match="owner_lost"):
            bridge.CodexResearchProvider(5, executable=Path("codex.exe")).ask(payload, directory, lost)
    finally:
        timer.cancel()
    assert len(offline_cli) == 1
    assert offline_cli[0][2].poll() is not None


def test_lease_renews_while_provider_runs_and_fences_changed_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, "RESEARCH_RENEW_SECONDS", 0.02)
    api = Api()
    original = api.renew_research

    def renew(*args):
        row = original(*args)
        if api.renewals >= 2:
            row["claim_generation"] = 2
        return row

    api.renew_research = renew
    with bridge.ResearchLease(api, "research-1", "worker", 1) as lease:
        assert lease.lost.wait(timeout=1)
    assert api.renewals == 2


def test_different_completed_output_is_rejected_not_relabelled(tmp_path):
    api = Api()
    api.complete_research = lambda *_args: {"request_id": "research-1", "status": "completed", "source": "deepseek_web", "raw_sha256": "x"}
    provider = Provider()
    with pytest.raises(bridge.ProviderUnavailable, match="receipt_mismatch"):
        bridge.process_research_job(api, provider, job=job(), worker_id="worker", state_root=tmp_path)
    assert provider.calls == 1


def test_research_mode_never_constructs_legacy_chat_or_browser_provider(monkeypatch):
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_HOST", raising=False)
    monkeypatch.setenv("PROBIGA_AI_BRIDGE_TOKEN", "test")
    monkeypatch.setattr(sys, "argv", ["bridge", "--research-only", "--once", "--server-url", "http://example.invalid"])
    monkeypatch.setattr(bridge, "run_research_worker", lambda args, token: 17)
    monkeypatch.setattr(bridge, "DeepSeekWebProvider", lambda *_args, **_kwargs: pytest.fail("webpage provider constructed"))
    monkeypatch.setattr(bridge, "CodexTaskProvider", lambda *_args, **_kwargs: pytest.fail("existing chat provider constructed"))
    assert bridge.main() == 17


def test_unconfirmed_child_termination_is_never_published_as_terminal_failure(tmp_path):
    class Unconfirmed:
        def ask(self, *_args):
            raise bridge.ResearchTerminationUnconfirmed("termination_unconfirmed")
    api = Api()
    with pytest.raises(bridge.ResearchTerminationUnconfirmed):
        bridge.process_research_job(api, Unconfirmed(), job=job(), worker_id="worker", state_root=tmp_path)
    assert api.posts == []
    receipt = json.loads((tmp_path / "research-1" / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "BLOCKED"
    assert receipt["stage"] == "termination_unconfirmed"
