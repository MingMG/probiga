"""Exercise the Windows simulation worker and embedded entries without QMT or DB."""
from __future__ import annotations

import ast
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.common.qmt_linux_ingest_protocol import canonical_sha256
from server.engine import qmt_strategy_simulation as simulation
from server.api import qmt_strategy_results as result_service
from server.common import scheduler_validation
from tools import install_qmt_simulation_entries as installer
from tools import run_qmt_strategy_daily as worker
from tools import ensure_qmt_windows_runtime as runtime
from tools.run_qmt_linux_ingest import QmtLinuxIngestClientError


BUILD = "a" * 40
SNAPSHOT_ID = "b" * 32
TARGET = "2026-09-30"
ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / installer.TEMPLATE


def heartbeat(*, native_build=BUILD, **changes):
    from integrations.bigqmt.release_identity import render_strategy_artifact
    from server.common.qmt_strategy_bridge_proof import STRATEGY_SOURCE, DIRECT_MODEL_SOURCE

    source = (ROOT / STRATEGY_SOURCE).read_bytes()
    direct = (ROOT / DIRECT_MODEL_SOURCE).read_bytes()
    source_hash = hashlib.sha256(source).hexdigest()
    blob = hashlib.sha1(b"blob " + str(len(source)).encode("ascii") + b"\0" + source).hexdigest()
    rendered = render_strategy_artifact(source, build_sha=native_build, git_blob=blob, source_sha256=source_hash)
    return {
        "updated_ts": 1000.0, "updated_at": "2026-10-08 22:50:00",
        "status": "running", "source": "gj_big_qmt_inner",
        "model_instance_id": "c" * 32, "strategy_build_sha": native_build,
        "strategy_git_blob": blob, "strategy_source_sha256": source_hash,
        "strategy_artifact_sha256": rendered["artifact_sha256"],
        "strategy_loaded_identity_sha256": rendered["identity_sha256"],
        "direct_acquisition_model_sha256": hashlib.sha256(direct).hexdigest(),
        "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
        **changes,
    }


def snapshot():
    value = {
        "schema": simulation.INPUT_SCHEMA, "trade_date": TARGET, "mode": "REPLAY",
        "prepared_at": "2026-10-08T22:50:00+08:00", "decision_at": TARGET + "T23:59:59",
        "market_clock": {"expected_trade_date": TARGET,
                         "current_closed_trade_date": "2026-10-08",
                         "observed_at": "2026-10-08T22:50:00+08:00"},
        "formula_contract": simulation.formula_contract(),
        "simulation_only": True, "real_order_allowed": False,
        "v2": {"status": "DATA_BLOCKED", "trade_date": TARGET,
               "reasons": ["PIT_BATCH_MISSING"], "frames": {}, "proofs": {}},
        "v3": {"status": "DATA_BLOCKED", "trade_date": TARGET,
               "reasons": ["QMT_DAILY_WINDOW_MISSING"], "stocks": [], "market_features": {}},
    }
    value["input_hash"] = simulation.snapshot_input_hash(value)
    return value


def issued(value):
    return {"status": "ISSUED", "snapshot_id": SNAPSHOT_ID, "edge_build_sha": BUILD,
            "trade_date": TARGET, "run_mode": "REPLAY", "issued_at": "2026-10-08T14:50:00Z",
            "snapshot_sha256": canonical_sha256(value), "snapshot": value,
            "simulation_only": True, "real_order_allowed": False,
            "automatic_real_order_submission": False, "real_order_authority": False}


def ack(payload, **changes):
    return {"status": "COMMITTED", "snapshot_id": payload["snapshot_id"],
            "run_uid": payload["snapshot_id"], "trade_date": payload["result"]["trade_date"],
            "result_hash": canonical_sha256(payload["result"]),
            "execution_hash": canonical_sha256(payload["execution"]),
            "simulation_only": True, "real_order_allowed": False,
            "automatic_real_order_submission": False, "real_order_authority": False,
            **changes}


class Client:
    def __init__(self, value=None, *, fail_upload=False, ack_changes=None, issued_changes=None):
        self.value = value
        self.fail_upload = fail_upload
        self.ack_changes = ack_changes or {}
        self.issued_changes = issued_changes or {}
        self.calls = []
        self.closed = False

    def close(self):
        self.closed = True

    def post(self, endpoint, payload, **_kwargs):
        self.calls.append((endpoint, deepcopy(payload)))
        if endpoint.endswith("/strategy-inputs"):
            return {**issued(self.value), "request_id": payload["request_id"], **self.issued_changes}
        assert endpoint == "/api/qmt-ingest/strategy-results"
        if self.fail_upload:
            raise RuntimeError("simulated transport failure")
        return ack(payload, **self.ack_changes)


def configure(monkeypatch, tmp_path, *, value=None, client=None, heartbeats=None):
    value = snapshot() if value is None else value
    client = Client(value) if client is None else client
    paths = {"root": tmp_path / "userdata/probiga_bridge",
             "heartbeat": tmp_path / "userdata/probiga_bridge/heartbeat.json"}
    monkeypatch.setattr(worker, "runtime_component_build_sha", lambda role, expected=None: BUILD)
    monkeypatch.setattr(worker, "verify_checkout", lambda build: None)
    monkeypatch.setattr(runtime, "validate_runtime", lambda build: None)
    monkeypatch.setattr(worker, "single_worker", lambda _state: nullcontext())
    monkeypatch.setattr(worker, "resolve_big_qmt_home", lambda **_kwargs: tmp_path)
    monkeypatch.setattr(worker, "bridge_paths", lambda _home: paths)
    monkeypatch.setattr(worker, "get_ai_bridge_config", lambda: {"token": "unit-test-machine-secret"})
    monkeypatch.setattr(worker, "Client", lambda *_args, **_kwargs: client)
    monkeypatch.setattr(worker.time, "time", lambda: 1001.0)
    values = iter(heartbeats or [heartbeat(), heartbeat()])
    monkeypatch.setattr(worker, "read_json", lambda _path: next(values))
    return client, tmp_path / "userdata/probiga_strategy_simulation"


def run(**kwargs):
    return worker.run(server_url="https://linux.example.test", trade_date=TARGET,
                      expected_build_sha=BUILD, **kwargs)


@pytest.mark.parametrize("changes", [
    {"status": "stopped"}, {"updated_ts": 909.0}, {"updated_ts": 1002.0},
    {"updated_ts": None}, {"updated_ts": True}, {"updated_ts": float("nan")},
    {"source": "another-provider"},
    {"strategy_identity_frozen": False}, {"strategy_identity_status": "UNAVAILABLE"},
])
def test_heartbeat_rejects_stale_stopped_future_and_unbound_identity(changes):
    with pytest.raises(worker.SimulationRuntimeError, match="QMT_BRIDGE_NOT_FRESH_BOUND_CONTENT"):
        worker.verify_bridge_identity(heartbeat(**changes), BUILD, now_ts=1001.0)


@pytest.mark.parametrize("field,value", [
    ("strategy_build_sha", "f" * 40), ("strategy_source_sha256", "f" * 64),
    ("strategy_artifact_sha256", "f" * 64), ("direct_acquisition_model_sha256", "f" * 64),
])
def test_format_valid_but_false_frozen_content_never_is_compatible(field, value):
    with pytest.raises(worker.SimulationRuntimeError, match="QMT_BRIDGE_FROZEN_CONTENT_DIFFERS"):
        worker.verify_bridge_identity(heartbeat(**{field: value}), BUILD, now_ts=1001.0)


def test_content_identical_old_native_build_retains_actual_raw_identity():
    raw = heartbeat(native_build="f" * 40)
    captured = worker.verify_bridge_identity(raw, BUILD, now_ts=1001.0)
    assert captured["strategy_build_sha"] == "f" * 40
    assert captured["strategy_artifact_sha256"] == raw["strategy_artifact_sha256"]
    assert set(captured) == set(worker.IDENTITY_FIELDS)


@pytest.mark.parametrize("field", [
    "model_instance_id", "strategy_git_blob", "strategy_source_sha256",
    "strategy_artifact_sha256", "strategy_loaded_identity_sha256",
    "direct_acquisition_model_sha256",
])
def test_heartbeat_requires_every_frozen_identity_hash(field):
    with pytest.raises(worker.SimulationRuntimeError, match="QMT_BRIDGE_IDENTITY_INCOMPLETE"):
        worker.verify_bridge_identity(heartbeat(**{field: "unbound"}), BUILD, now_ts=1001.0)


def test_heartbeat_timestamp_is_bound_to_the_native_observation():
    value = worker.verify_bridge_identity(heartbeat(updated_at="forged-current-text"), BUILD, now_ts=1001.0)
    assert value["updated_at"] == datetime.fromtimestamp(1000.0, worker.SHANGHAI).isoformat(timespec="seconds")
    assert value["strategy_build_sha"] == BUILD


def test_busy_bridge_is_a_valid_native_model_identity():
    value = worker.verify_bridge_identity(heartbeat(status="busy"), BUILD, now_ts=1001.0)
    assert value["model_instance_id"] == "c" * 32


@pytest.mark.parametrize("strategy_key", ["intraday_surprise", "weak_market_structural_mainline", "unknown"])
def test_excluded_and_unknown_keys_never_request_inputs(monkeypatch, tmp_path, strategy_key):
    client, _state = configure(monkeypatch, tmp_path)
    with pytest.raises(worker.SimulationRuntimeError, match="EXCLUDED_OR_UNKNOWN_STRATEGY_KEY"):
        run(strategy_key=strategy_key)
    assert client.calls == []


def test_unknown_execution_origin_never_requests_inputs(monkeypatch, tmp_path):
    client, _state = configure(monkeypatch, tmp_path)
    with pytest.raises(worker.SimulationRuntimeError, match="UNKNOWN_EXECUTION_ORIGIN"):
        run(origin="untrusted")
    assert client.calls == []


def test_exact_checkout_rejects_revision_mismatch_and_dirty_formula_sources(monkeypatch):
    monkeypatch.setattr(worker.subprocess, "check_output", lambda *_args, **_kwargs: "f" * 40)
    with pytest.raises(worker.SimulationRuntimeError, match="WINDOWS_CHECKOUT_BUILD_DIFFERS"):
        worker.verify_checkout(BUILD)
    outputs = iter([BUILD + "\n", " M server/engine/qmt_strategy_simulation.py\n"])
    monkeypatch.setattr(worker.subprocess, "check_output", lambda *_args, **_kwargs: next(outputs))
    with pytest.raises(worker.SimulationRuntimeError, match="WINDOWS_FORMULA_SOURCE_DIRTY"):
        worker.verify_checkout(BUILD)
    with pytest.raises(worker.SimulationRuntimeError, match="WINDOWS_BUILD_IDENTITY_REQUIRED"):
        worker.verify_checkout("invalid")


def test_mock_client_executes_all_ten_and_four_without_inventing_picks(monkeypatch, tmp_path):
    client, state = configure(monkeypatch, tmp_path)
    result = run()
    assert result["status"] == "completed"
    assert len(result["rows"]) == 14
    assert {row["strategy_key"] for row in result["rows"]}.isdisjoint(simulation.EXCLUDED_KEYS)
    assert all(row["status"] == "DATA_BLOCKED" and row["selected"] == [] for row in result["rows"])
    posted = [payload for endpoint, payload in client.calls if endpoint.endswith("/strategy-results")][0]
    assert len(posted["result"]["strategy_rows"]) == 10
    assert len(posted["result"]["combination_rows"]) == 4
    assert posted["execution"]["origin"] == "WINDOWS_DAILY"
    assert result["simulation_only"] is True and result["automatic_real_order_submission"] is False
    assert result["result_hash"] == canonical_sha256(posted["result"])
    assert not list(state.glob("*.pending.json"))
    assert (state / (SNAPSHOT_ID + ".committed.json")).exists()
    assert (state / (SNAPSHOT_ID + ".receipt.json")).exists()
    assert client.closed


def test_qmt_entry_filters_console_rows_but_keeps_one_full_calculation(monkeypatch, tmp_path):
    client, _state = configure(monkeypatch, tmp_path)
    result = run(strategy_key="main_wave", origin="QMT_ENTRY")
    assert len(result["rows"]) == 1 and result["rows"][0]["strategy_key"] == "main_wave"
    posted = client.calls[-1][1]
    assert len(posted["result"]["strategy_rows"]) == 10
    assert len(posted["result"]["combination_rows"]) == 4
    assert posted["execution"]["origin"] == "QMT_ENTRY"


def test_tampered_snapshot_hash_never_uploads_result(monkeypatch, tmp_path):
    value = snapshot()
    value["v2"]["reasons"] = ["altered-after-issue"]
    client, state = configure(monkeypatch, tmp_path, value=value)
    with pytest.raises(ValueError, match="input hash differs"):
        run()
    assert all(not endpoint.endswith("/strategy-results") for endpoint, _payload in client.calls)
    assert not list(state.glob("*.pending.json"))
    assert client.closed


@pytest.mark.parametrize("changes", [
    {"status": "COMMITTED"}, {"snapshot_id": "invalid"}, {"edge_build_sha": "9" * 40},
    {"snapshot": None}, {"trade_date": "2026-09-29"}, {"run_mode": "DAILY"},
    {"snapshot_sha256": "0" * 64}, {"simulation_only": False}, {"real_order_allowed": True},
])
def test_bad_issued_envelope_never_evaluates_or_uploads(monkeypatch, tmp_path, changes):
    client = Client(snapshot(), issued_changes=changes)
    configure(monkeypatch, tmp_path, client=client)
    monkeypatch.setattr(worker, "evaluate_snapshot", lambda _value: pytest.fail("unsafe issued input was evaluated"))
    reason = ("SIGNED_INPUT_JOB_IDENTITY_DIFFERS" if set(changes) & {"edge_build_sha", "simulation_only", "real_order_allowed"}
              else "SIGNED_INPUT_JOB_STATE_INVALID" if changes.get("status") == "COMMITTED"
              else "SIGNED_INPUT_IDENTITY_DIFFERS")
    with pytest.raises(worker.SimulationRuntimeError, match=reason):
        run()
    assert len(client.calls) == 1 and client.calls[0][0].endswith("/strategy-inputs")
    assert client.closed


def test_signed_input_cannot_silently_change_requested_trade_date(monkeypatch, tmp_path):
    client, _state = configure(monkeypatch, tmp_path)
    with pytest.raises(worker.SimulationRuntimeError, match="SIGNED_INPUT_REQUEST_DATE_DIFFERS"):
        worker.run(server_url="https://linux.example.test", trade_date="2026-09-29", expected_build_sha=BUILD)
    assert len(client.calls) == 1 and client.closed


@pytest.mark.parametrize("strategy_key,expected_rows", [("", 14), ("main_wave", 1)])
def test_committed_input_reuses_original_execution_without_second_result_post(monkeypatch, tmp_path, strategy_key, expected_rows):
    payload = retained_payload()
    previous = ack(payload, execution=deepcopy(payload["execution"]))
    client = Client(snapshot(), issued_changes={"committed_receipt": previous})
    _client, state = configure(monkeypatch, tmp_path, client=client)
    returned = run(strategy_key=strategy_key, origin="QMT_ENTRY")
    assert returned["status"] == "reused" and returned["run_uid"] == SNAPSHOT_ID
    assert returned["original_execution"] == payload["execution"]
    assert returned["original_execution"]["origin"] == "WINDOWS_DAILY"
    assert len(returned["rows"]) == expected_rows
    assert len(client.calls) == 1 and client.calls[0][0].endswith("/strategy-inputs")
    assert not list(state.glob("*.pending.json")) and client.closed


@pytest.mark.parametrize("changes,reason", [
    ({"execution": None}, "SIGNED_REUSED_EXECUTION_DIFFERS"),
    ({"result_hash": "0" * 64}, "SIGNED_RESULT_ACK_HASH_DIFFERS"),
    ({"execution_hash": "0" * 64}, "SIGNED_RESULT_ACK_HASH_DIFFERS"),
    ({"automatic_real_order_submission": True}, "SIGNED_RESULT_ACK_HASH_DIFFERS"),
])
def test_unsafe_reused_receipt_is_not_reported_as_original_result(monkeypatch, tmp_path, changes, reason):
    payload = retained_payload()
    previous = ack(payload, **{"execution": deepcopy(payload["execution"]), **changes})
    client = Client(snapshot(), issued_changes={"committed_receipt": previous})
    configure(monkeypatch, tmp_path, client=client)
    with pytest.raises(worker.SimulationRuntimeError, match=reason):
        run(origin="QMT_ENTRY")
    assert len(client.calls) == 1 and client.closed


def test_model_change_during_evaluation_never_uploads(monkeypatch, tmp_path):
    client, state = configure(monkeypatch, tmp_path,
                              heartbeats=[heartbeat(), heartbeat(model_instance_id="3" * 32)])
    with pytest.raises(worker.SimulationRuntimeError, match="QMT_MODEL_CHANGED_DURING_EXECUTION"):
        run()
    assert all(not endpoint.endswith("/strategy-results") for endpoint, _payload in client.calls)
    assert not list(state.glob("*.pending.json"))
    assert client.closed


def retained_payload():
    value = snapshot()
    return {"schema": worker.COMMIT_SCHEMA, "edge_build_sha": BUILD,
            "snapshot_id": SNAPSHOT_ID, "result": simulation.evaluate_snapshot(value),
            "execution": {"origin": "WINDOWS_DAILY", "started_at": "2026-10-08T22:50:00+08:00",
                          "finished_at": "2026-10-08T22:50:05+08:00",
                          "bridge_identity": worker.verify_bridge_identity(heartbeat(), BUILD, now_ts=1001.0)}}


def test_transient_upload_preserves_exact_retained_bytes_and_retry_is_idempotent(tmp_path):
    payload = retained_payload()
    path = tmp_path / (SNAPSHOT_ID + ".pending.json")
    worker.atomic_json(path, payload)
    original = path.read_bytes()
    client = Client(fail_upload=True)
    with pytest.raises(RuntimeError, match="transport failure"):
        worker.publish_retained(client, tmp_path, BUILD)
    assert path.read_bytes() == original
    assert not list(tmp_path.glob("*.receipt.json"))
    client.fail_upload = False
    receipts = worker.publish_retained(client, tmp_path, BUILD)
    assert receipts[0]["result_hash"] == canonical_sha256(payload["result"])
    assert client.calls[0][1] == client.calls[1][1] == payload
    assert (tmp_path / (SNAPSHOT_ID + ".committed.json")).read_bytes() == original
    assert worker.publish_retained(client, tmp_path, BUILD) == []
    assert len(client.calls) == 2


@pytest.mark.parametrize("changes", [
    {"status": "ISSUED"}, {"snapshot_id": "0" * 32}, {"run_uid": "0" * 32},
    {"trade_date": "2026-09-29"}, {"simulation_only": False}, {"real_order_allowed": True},
    {"result_hash": "0" * 64}, {"execution_hash": "0" * 64},
    {"automatic_real_order_submission": True}, {"real_order_authority": True},
])
def test_mismatched_ack_never_marks_pending_as_committed(tmp_path, changes):
    path = tmp_path / (SNAPSHOT_ID + ".pending.json")
    worker.atomic_json(path, retained_payload())
    with pytest.raises(worker.SimulationRuntimeError, match="SIGNED_RESULT_ACK_HASH_DIFFERS"):
        worker.publish_retained(Client(ack_changes=changes), tmp_path, BUILD)
    assert path.exists() and not list(tmp_path.glob("*.receipt.json"))


def test_prior_release_pending_results_are_preserved_without_rebinding(tmp_path):
    payload = retained_payload()
    payload["edge_build_sha"] = "9" * 40
    path = tmp_path / (SNAPSHOT_ID + ".pending.json")
    worker.atomic_json(path, payload)
    original = path.read_bytes()
    client = Client()
    assert worker.publish_retained(client, tmp_path, BUILD) == []
    assert client.calls == [] and path.read_bytes() == original


def scheduler_evidence():
    value = snapshot()
    value["mode"] = "DAILY"
    value["market_clock"]["current_closed_trade_date"] = TARGET
    value["input_hash"] = simulation.snapshot_input_hash(value)
    result = simulation.evaluate_snapshot(value)
    result_hash = canonical_sha256(result)
    detail = {"run_uid": SNAPSHOT_ID, "snapshot_id": SNAPSHOT_ID, "edge_build_sha": BUILD,
              "trade_date": TARGET, "run_mode": "DAILY", "status": "DATA_BLOCKED", "result": result,
              "result_hash": result_hash, "simulation_only": True, "real_order_allowed": False,
              "selected_count": 0, "blocked_count": 14,
              "execution": {"origin": "WINDOWS_DAILY", "started_at": "2026-10-08T22:50:00+08:00"}}
    receipt = {"schema": "probiga.qmt-strategy-worker-receipt.v1", "status": "completed",
               "trade_date": TARGET, "snapshot_id": SNAPSHOT_ID, "run_uid": SNAPSHOT_ID,
               "result_hash": result_hash, "simulation_only": True,
               "automatic_real_order_submission": False}
    return detail, receipt


def validate_scheduler(monkeypatch, detail, receipt):
    engine = object()
    reads = []

    def read_saved(run_uid, **kwargs):
        reads.append((run_uid, kwargs))
        assert kwargs["engine"] is engine
        return detail

    monkeypatch.setattr(result_service, "read_strategy_run", read_saved)
    monkeypatch.setattr(scheduler_validation, "authoritative_closed_trade_date", lambda *_args, **_kwargs: TARGET)
    validated = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "qmt_strategy_simulation_daily", "_scheduler_expected_build_sha": BUILD},
        engine=engine, started_at=datetime(2026, 10, 8, 22, 50), now=datetime(2026, 10, 8, 22, 51),
        output=json.dumps(receipt))
    return validated, reads


def test_scheduler_reads_saved_exact_result_and_preserves_data_blocked_success(monkeypatch):
    detail, receipt = scheduler_evidence()
    validated, reads = validate_scheduler(monkeypatch, detail, receipt)
    assert validated.checked and validated.ok
    assert "DATA_BLOCKED" in validated.message and "blocked=14" in validated.message
    assert reads[0][0] == SNAPSHOT_ID


@pytest.mark.parametrize("kind,field,value", [
    ("receipt", "status", "error"), ("receipt", "schema", "counterfeit"),
    ("receipt", "run_uid", "9" * 32), ("receipt", "simulation_only", False),
    ("receipt", "automatic_real_order_submission", True), ("receipt", "result_hash", "0" * 64),
    ("receipt", "status", "reused"), ("detail", "run_mode", "REPLAY"),
    ("receipt", "trade_date", "2026-09-29"), ("detail", "edge_build_sha", "9" * 40),
    ("detail", "result_hash", "0" * 64), ("detail", "trade_date", "2026-09-29"),
    ("detail", "simulation_only", False), ("detail", "real_order_allowed", True),
    ("execution", "origin", "QMT_ENTRY"),
    ("execution", "started_at", "2026-10-08T22:49:49+08:00"),
    ("execution", "started_at", "2026-10-08T22:50:00"),
    ("execution", "started_at", "2026-10-08T22:51:06+08:00"),
])
def test_scheduler_rejects_counterfeit_build_hash_origin_and_old_execution(monkeypatch, kind, field, value):
    detail, receipt = scheduler_evidence()
    target = receipt if kind == "receipt" else detail if kind == "detail" else detail["execution"]
    target[field] = value
    validated, _reads = validate_scheduler(monkeypatch, detail, receipt)
    assert validated.checked and not validated.ok
    assert validated.message.startswith("QMT simulation receipt validation failed:")


def test_scheduler_rejects_saved_result_mutation_despite_matching_printed_hash(monkeypatch):
    detail, receipt = scheduler_evidence()
    detail["result"]["strategy_rows"][0]["blocked_reasons"] = ["altered saved facts"]
    validated, _reads = validate_scheduler(monkeypatch, detail, receipt)
    assert validated.checked and not validated.ok


@pytest.mark.parametrize("changes", [{"simulation_only": False}, {"real_order_allowed": True}])
def test_scheduler_rejects_hash_consistent_unsafe_result_flags(monkeypatch, changes):
    detail, receipt = scheduler_evidence()
    detail["result"].update(changes)
    detail["result_hash"] = receipt["result_hash"] = canonical_sha256(detail["result"])
    validated, _reads = validate_scheduler(monkeypatch, detail, receipt)
    assert validated.checked and not validated.ok


def test_scheduler_fails_closed_when_saved_result_is_unavailable(monkeypatch):
    _detail, receipt = scheduler_evidence()
    monkeypatch.setattr(result_service, "read_strategy_run", lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyError("not persisted")))
    validated = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "qmt_strategy_simulation_daily", "_scheduler_expected_build_sha": BUILD},
        engine=object(), started_at=datetime(2026, 10, 8, 22, 50), output=json.dumps(receipt))
    assert validated.checked and not validated.ok


def test_template_renders_literal_identity_and_python36_without_shell_or_secrets(tmp_path):
    rendered = installer.render_entry(TEMPLATE.read_bytes(), strategy_key="main_wave",
                                      name="主升浪趋势", release_path=tmp_path / "模拟发布.json",
                                      build_sha=BUILD)
    source = rendered.decode("utf-8")
    assert "__PROBIGA_" not in source
    tree = ast.parse(source, feature_version=(3, 6))
    constants = {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
                 if isinstance(node, ast.Assign) and len(node.targets) == 1
                 and isinstance(node.targets[0], ast.Name) and node.targets[0].id.isupper()}
    assert constants["EXPECTED_BUILD"] == BUILD
    assert constants["STRATEGY_NAME"] == "主升浪趋势"
    assert constants["RELEASE_PATH"] == str(tmp_path / "模拟发布.json")
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    names = {node.func.id.lower() if isinstance(node.func, ast.Name) else node.func.attr.lower()
             for node in calls if isinstance(node.func, (ast.Name, ast.Attribute))}
    assert names.isdisjoint({"passorder", "cancel_order", "order_stock", "set_account", "system", "exec", "eval"})
    popen = next(node for node in calls if isinstance(node.func, ast.Attribute) and node.func.attr == "Popen")
    assert not any(keyword.arg == "shell" and ast.literal_eval(keyword.value) is True for keyword in popen.keywords)
    assert not any(secret in source for secret in ("MYSQL_URL", "AI_BRIDGE_TOKEN", "GM_TOKEN", "password", "bearer"))


def test_installer_writes_exactly_fourteen_bound_entries_and_readback_hashes(monkeypatch, tmp_path):
    root = tmp_path / "release"
    runner = root / "tools/run_qmt_strategy_daily.py"
    runner.parent.mkdir(parents=True)
    runner.write_bytes(b"# worker fixture\n")
    source = TEMPLATE.read_bytes()
    template_copy = root / installer.TEMPLATE
    template_copy.parent.mkdir(parents=True)
    template_copy.write_bytes(source)
    monkeypatch.setattr(installer, "ROOT", root)
    monkeypatch.setattr(installer.subprocess, "check_output", lambda args, **_kwargs: BUILD if "rev-parse" in args else "")
    def exact_artifact(*, source_path, **_kwargs):
        encoded = source if source_path == template_copy else runner.read_bytes()
        return {"source_bytes": encoded, "source_sha256": hashlib.sha256(encoded).hexdigest(), "git_blob": "e" * 40}
    monkeypatch.setattr(installer, "git_strategy_artifact", exact_artifact)
    result = installer.install_entries(qmt_home=tmp_path / "qmt", expected_build_sha=BUILD)
    assert len(result["entries"]) == 14
    assert {row["strategy_key"] for row in result["entries"]}.isdisjoint(simulation.EXCLUDED_KEYS)
    assert result["runner_sha256"] == hashlib.sha256(runner.read_bytes()).hexdigest()
    for entry in result["entries"]:
        encoded = Path(entry["path"]).read_bytes()
        assert hashlib.sha256(encoded).hexdigest() == entry["sha256"]
        assert b"__PROBIGA_" not in encoded
        ast.parse(encoded.decode("utf-8"), feature_version=(3, 6))
    manifest = json.loads(Path(result["manifest_path"]).read_text("utf-8"))
    assert manifest["simulation_only"] is True and manifest["automatic_real_order_submission"] is False
    assert len(manifest["entries"]) == 14


def test_installer_rejects_wrong_release_head_before_writing_entries(monkeypatch, tmp_path):
    monkeypatch.setattr(installer.subprocess, "check_output", lambda *_args, **_kwargs: "9" * 40)
    with pytest.raises(RuntimeError, match="exact release HEAD"):
        installer.install_entries(qmt_home=tmp_path / "qmt", expected_build_sha=BUILD)
    assert not (tmp_path / "qmt").exists()


def test_embedded_entry_only_launches_bound_worker_argv_and_reports_its_receipt(monkeypatch, tmp_path, capsys):
    root = tmp_path / "release"
    runner = root / "tools/run_qmt_strategy_daily.py"
    python = root / "runtime/qmt-py313/Scripts/python.exe"
    runner.parent.mkdir(parents=True)
    python.parent.mkdir(parents=True)
    runner.write_bytes(b"# bound fixture\n")
    python.write_bytes(b"fixture-not-an-executable")
    release_path = tmp_path / "simulation.json"
    worker.atomic_json(release_path, {
        "schema": "probiga.qmt-simulation-entries.v1", "build_sha": BUILD,
        "simulation_only": True, "production_root": str(root),
        "runner_sha256": hashlib.sha256(runner.read_bytes()).hexdigest(),
        "state_root": str(tmp_path / "state")})
    rendered = installer.render_entry(TEMPLATE.read_bytes(), strategy_key="main_wave",
                                      name="主升浪趋势", release_path=release_path, build_sha=BUILD)
    namespace = {"__name__": "qmt_test_entry"}
    exec(compile(rendered.decode("utf-8"), "qmt-entry", "exec"), namespace)
    calls = []

    def popen(argv, **kwargs):
        calls.append((argv, kwargs))
        kwargs["stdout"].write(json.dumps({"status": "completed", "simulation_only": True}) + "\n")
        return SimpleNamespace(poll=lambda: 0)

    namespace["subprocess"] = SimpleNamespace(Popen=popen, CREATE_NO_WINDOW=0)
    timers = []
    context = SimpleNamespace(run_time=lambda *args: timers.append(args))
    namespace["init"](context)
    namespace["handlebar"](context)
    argv, kwargs = calls[0]
    assert argv == [str(python), "-P", str(runner), "--expected-build-sha", BUILD,
                    "--strategy-key", "main_wave", "--origin", "QMT_ENTRY", "--json"]
    assert kwargs.get("shell", False) is False
    assert kwargs["env"]["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] == "qmt_windows_edge"
    assert kwargs["env"]["PROBIGA_BUILD_COMMIT_SHA"] == BUILD
    assert timers[0][0] == "probiga_sim_poll"
    assert '"simulation_only": true' in capsys.readouterr().out
    assert namespace["_reported"] is True and namespace["_output"] is None


def test_main_never_prints_raw_transport_credentials(monkeypatch, capsys):
    monkeypatch.setattr(worker, "load_project_env", lambda *_args: None)
    monkeypatch.setattr(worker, "run", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("bearer=private-secret")))
    assert worker.main(["--server-url", "https://linux.example.test"]) == 2
    output = capsys.readouterr().out
    assert "private-secret" not in output
    assert json.loads(output)["reason"] == "SIMULATION_EXECUTION_OR_UPLOAD_FAILED"
