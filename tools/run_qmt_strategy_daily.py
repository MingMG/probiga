"""One Windows-owned, order-free execution path for QMT strategy research.

QMT's embedded Python entries invoke this same build-bound worker as the daily
scheduler. Linux issues frozen fact inputs and verifies the returned calculation;
it does not execute a second daily selection task or replace formal governance.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.spool import bridge_paths, read_json, resolve_big_qmt_home
from server.common.component_release import runtime_component_build_sha
from server.common.config import get_ai_bridge_config
from server.common.qmt_linux_ingest_protocol import canonical_json, canonical_sha256
from server.common.qmt_strategy_bridge_proof import validate_qmt_strategy_bridge_identity
from server.engine.qmt_strategy_simulation import evaluate_snapshot, strategy_catalog
from tools.env_config import load_project_env
from tools.run_qmt_linux_ingest import Client, QmtLinuxIngestClientError

SHANGHAI = ZoneInfo("Asia/Shanghai")
INPUT_REQUEST_SCHEMA = "probiga.qmt-strategy-input-request.v1"
COMMIT_SCHEMA = "probiga.qmt-strategy-result-commit.v1"
PREPARATION_BUDGET_SECONDS = 3600
PREPARATION_POLL_SECONDS = 5
IDENTITY_FIELDS = (
    "source", "model_instance_id", "strategy_build_sha", "strategy_git_blob",
    "strategy_source_sha256", "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256", "direct_acquisition_model_sha256",
    "strategy_identity_frozen", "strategy_identity_status", "updated_at",
)


class SimulationRuntimeError(RuntimeError):
    pass


def now_text() -> str:
    return datetime.now(SHANGHAI).isoformat(timespec="seconds")


def verify_bridge_identity(heartbeat: dict, build_sha: str, *, now_ts: float | None = None) -> dict:
    """Reject stale or changed native content; attest its actual frozen build."""
    current = time.time() if now_ts is None else now_ts
    timestamp = heartbeat.get("updated_ts")
    if (type(timestamp) not in (int, float) or not 0 <= current - timestamp <= 90
            or heartbeat.get("status") not in {"running", "busy"}
            or heartbeat.get("source") != "gj_big_qmt_inner"
            or heartbeat.get("strategy_identity_frozen") is not True
            or heartbeat.get("strategy_identity_status") != "BOUND"):
        raise SimulationRuntimeError("QMT_BRIDGE_NOT_FRESH_BOUND_CONTENT")
    patterns = {
        "model_instance_id": r"[0-9a-f]{32}", "strategy_build_sha": r"[0-9a-f]{40}",
        "strategy_git_blob": r"[0-9a-f]{40}",
        "strategy_source_sha256": r"[0-9a-f]{64}",
        "strategy_artifact_sha256": r"[0-9a-f]{64}",
        "strategy_loaded_identity_sha256": r"[0-9a-f]{64}",
        "direct_acquisition_model_sha256": r"[0-9a-f]{64}",
    }
    if any(re.fullmatch(pattern, str(heartbeat.get(key) or "")) is None
           for key, pattern in patterns.items()):
        raise SimulationRuntimeError("QMT_BRIDGE_IDENTITY_INCOMPLETE")
    identity = {key: heartbeat[key] for key in IDENTITY_FIELDS}
    identity["updated_at"] = datetime.fromtimestamp(timestamp, SHANGHAI).isoformat(timespec="seconds")
    try:
        validate_qmt_strategy_bridge_identity({**heartbeat, **identity}, expected_app_build_sha=build_sha)
    except ValueError as exc:
        raise SimulationRuntimeError("QMT_BRIDGE_FROZEN_CONTENT_DIFFERS") from exc
    return identity


def verify_checkout(build_sha: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", build_sha or ""):
        raise SimulationRuntimeError("WINDOWS_BUILD_IDENTITY_REQUIRED")
    observed = subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True,
        timeout=15, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).strip()
    if observed != build_sha:
        raise SimulationRuntimeError("WINDOWS_CHECKOUT_BUILD_DIFFERS")
    # Formula and transport source must not come from an uncommitted checkout.
    changed = subprocess.check_output(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--",
         "server/engine/qmt_strategy_simulation.py", "server/trading_v3",
         "biz/analysis/sync_analysis_fast.py", "strategies", __file__,
         "server/common/qmt_strategy_bridge_proof.py",
         "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py",
         "acquisition/qmt_model.py"],
        text=True, timeout=15,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).strip()
    if changed:
        raise SimulationRuntimeError("WINDOWS_FORMULA_SOURCE_DIRTY")


def atomic_json(path: Path, payload: dict) -> None:
    encoded = canonical_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + "." + str(os.getpid()) + ".tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def single_worker(state_root: Path):
    """OS lock survives neither crashes nor restarts; never an unsafe stale-PID lock."""
    if os.name != "nt":
        raise SimulationRuntimeError("SIMULATION_EXECUTOR_MUST_BE_WINDOWS_QMT")
    import msvcrt
    state_root.mkdir(parents=True, exist_ok=True)
    with (state_root / "worker.lock").open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise SimulationRuntimeError("SIMULATION_WORKER_ALREADY_RUNNING") from exc
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def validate_issued(issued: dict, build_sha: str) -> tuple[str, dict]:
    snapshot_id = str(issued.get("snapshot_id") or "")
    snapshot = issued.get("snapshot")
    if (issued.get("status") != "ISSUED"
            or not re.fullmatch(r"[0-9a-f]{32}", snapshot_id)
            or issued.get("edge_build_sha") != build_sha
            or not isinstance(snapshot, dict)
            or issued.get("trade_date") != snapshot.get("trade_date")
            or issued.get("run_mode") != snapshot.get("mode")
            or issued.get("snapshot_sha256") != canonical_sha256(snapshot)
            or issued.get("simulation_only") is not True
            or issued.get("real_order_allowed") is not False
            or issued.get("automatic_real_order_submission") is not False
            or issued.get("real_order_authority") is not False):
        raise SimulationRuntimeError("SIGNED_INPUT_IDENTITY_DIFFERS")
    return snapshot_id, snapshot


def retained_input_request(state_root: Path, build_sha: str, trade_date: str) -> tuple[Path, dict]:
    """Resume one exact durable fact job, never create a second job on timeout."""
    matching = []
    for path in sorted(state_root.glob("*.input-request.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (set(payload) != {"schema", "request_id", "edge_build_sha", "trade_date"}
                or payload.get("schema") != INPUT_REQUEST_SCHEMA
                or not re.fullmatch(r"[0-9a-f]{32}", str(payload.get("request_id") or ""))
                or path.name != payload["request_id"] + ".input-request.json"):
            raise SimulationRuntimeError("RETAINED_INPUT_REQUEST_IDENTITY_DIFFERS")
        if payload["edge_build_sha"] == build_sha and payload["trade_date"] == trade_date:
            matching.append((path, payload))
    if len(matching) > 1:
        raise SimulationRuntimeError("MULTIPLE_RETAINED_INPUT_REQUESTS_FOR_ONE_SCOPE")
    if matching:
        return matching[0]
    payload = {"schema": INPUT_REQUEST_SCHEMA, "request_id": uuid.uuid4().hex,
               "edge_build_sha": build_sha, "trade_date": trade_date}
    path = state_root / (payload["request_id"] + ".input-request.json")
    atomic_json(path, payload)
    return path, payload


def prepare_issued_input(client: Client, state_root: Path, build_sha: str,
                         trade_date: str) -> tuple[Path, dict]:
    path, payload = retained_input_request(state_root, build_sha, trade_date)
    deadline = time.monotonic() + PREPARATION_BUDGET_SECONDS
    fixed_identity = None
    binding_path = path.with_name(path.name.replace(".input-request.json", ".input-binding.json"))
    if binding_path.exists():
        binding = json.loads(binding_path.read_text(encoding="utf-8"))
        if (set(binding) != {"request_id", "edge_build_sha", "trade_date", "run_mode"}
                or binding["request_id"] != payload["request_id"]
                or binding["edge_build_sha"] != build_sha):
            raise SimulationRuntimeError("RETAINED_INPUT_BINDING_IDENTITY_DIFFERS")
        fixed_identity = (binding["trade_date"], binding["run_mode"])
    while time.monotonic() < deadline:
        try:
            issued = client.post("/api/qmt-ingest/strategy-inputs", payload,
                                 deadline=deadline)
        except QmtLinuxIngestClientError as exc:
            # The client exposes only a typed transport description, not raw
            # HTTP bodies or secrets. Authentication/contract errors are final.
            description = str(exc)
            if (description != "Linux ingestion API is unavailable"
                    and not re.search(r"HTTP (500|502|503|504)$", description)):
                raise
            issued = None
        if issued is not None:
            if (not isinstance(issued, dict)
                    or issued.get("request_id") != payload["request_id"]
                    or issued.get("edge_build_sha") != build_sha
                    or issued.get("simulation_only") is not True
                    or issued.get("real_order_allowed") is not False
                    or issued.get("automatic_real_order_submission") is not False
                    or issued.get("real_order_authority") is not False):
                raise SimulationRuntimeError("SIGNED_INPUT_JOB_IDENTITY_DIFFERS")
            current_identity = (issued.get("trade_date"), issued.get("run_mode"))
            if (not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(current_identity[0] or ""))
                    or current_identity[1] not in {"DAILY", "REPLAY"}
                    or (fixed_identity is not None and fixed_identity != current_identity)):
                raise SimulationRuntimeError("SIGNED_INPUT_JOB_DATE_OR_MODE_CHANGED")
            fixed_identity = current_identity
            if not binding_path.exists():
                atomic_json(binding_path, {"request_id": payload["request_id"], "edge_build_sha": build_sha,
                                           "trade_date": current_identity[0], "run_mode": current_identity[1]})
            status = issued.get("status")
            if status == "ISSUED":
                validate_issued(issued, build_sha)
                return path, issued
            if status == "FAILED":
                atomic_json(path.with_name(path.name.replace(".input-request.json", ".input-failure.json")), issued)
                path.rename(path.with_name(path.name.replace(".input-request.json", ".input-failed.json")))
                code = str(issued.get("error_code") or "INPUT_PREPARATION_FAILED")
                if not re.fullmatch(r"[A-Z0-9_]{3,80}", code):
                    code = "INPUT_PREPARATION_FAILED"
                raise SimulationRuntimeError(code)
            if status not in {"QUEUED", "PREPARING"}:
                raise SimulationRuntimeError("SIGNED_INPUT_JOB_STATE_INVALID")
        remaining = deadline - time.monotonic()
        if remaining <= PREPARATION_POLL_SECONDS:
            break
        time.sleep(PREPARATION_POLL_SECONDS)
    raise SimulationRuntimeError("INPUT_PREPARATION_BUDGET_EXPIRED_REQUEST_RETAINED")


def complete_input_request(path: Path) -> None:
    path.rename(path.with_name(path.name.replace(".input-request.json", ".input-completed.json")))


def verify_ack(receipt: dict, payload: dict) -> None:
    if (receipt.get("status") != "COMMITTED"
            or receipt.get("snapshot_id") != payload["snapshot_id"]
            or receipt.get("run_uid") != payload["snapshot_id"]
            or receipt.get("trade_date") != payload["result"]["trade_date"]
            or receipt.get("simulation_only") is not True
            or receipt.get("real_order_allowed") is not False
            or receipt.get("automatic_real_order_submission") is not False
            or receipt.get("real_order_authority") is not False
            or receipt.get("execution_hash") != canonical_sha256(payload["execution"])
            or receipt.get("result_hash") != canonical_sha256(payload["result"])):
        raise SimulationRuntimeError("SIGNED_RESULT_ACK_HASH_DIFFERS")


def publish_retained(client: Client, state_root: Path, build_sha: str) -> list[dict]:
    """Retry only exact retained bytes; never discard a run on transient upload failure."""
    receipts = []
    for path in sorted(state_root.glob("*.pending.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("edge_build_sha") != build_sha:
            continue  # retained prior releases are evidence, never silently rewritten
        receipt = client.post("/api/qmt-ingest/strategy-results", payload)
        verify_ack(receipt, payload)
        atomic_json(path.with_name(path.name.replace(".pending.json", ".receipt.json")), receipt)
        path.rename(path.with_name(path.name.replace(".pending.json", ".committed.json")))
        receipts.append(receipt)
    return receipts


def run(*, server_url: str, trade_date: str = "", origin: str = "WINDOWS_DAILY",
        expected_build_sha: str | None = None, strategy_key: str = "") -> dict:
    catalog = strategy_catalog()
    allowed_keys = {row["strategy_key"] for row in catalog["strategies"] + catalog["combinations"]}
    if strategy_key and strategy_key not in allowed_keys:
        raise SimulationRuntimeError("EXCLUDED_OR_UNKNOWN_STRATEGY_KEY")
    if origin not in {"WINDOWS_DAILY", "QMT_ENTRY"}:
        raise SimulationRuntimeError("UNKNOWN_EXECUTION_ORIGIN")
    build_sha = runtime_component_build_sha("windows", expected_build_sha)
    from tools.ensure_qmt_windows_runtime import validate_runtime
    validate_runtime(build_sha)
    verify_checkout(build_sha)
    qmt_home = resolve_big_qmt_home(required=True)
    paths = bridge_paths(qmt_home)
    identity = verify_bridge_identity(read_json(paths["heartbeat"]), build_sha)
    state_root = paths["root"].parent / "probiga_strategy_simulation"
    secret = str(get_ai_bridge_config().get("token") or "")
    client = Client(server_url, secret, timeout=30)
    try:
        with single_worker(state_root):
            publish_retained(client, state_root, build_sha)
            input_path, issued = prepare_issued_input(client, state_root, build_sha, trade_date)
            snapshot_id, snapshot = validate_issued(issued, build_sha)
            if trade_date and snapshot["trade_date"] != trade_date:
                raise SimulationRuntimeError("SIGNED_INPUT_REQUEST_DATE_DIFFERS")
            started_at = now_text()
            result = evaluate_snapshot(snapshot)
            if result.get("simulation_only") is not True or result.get("real_order_allowed") is not False:
                raise SimulationRuntimeError("NON_SIMULATION_RESULT_FORBIDDEN")
            previous = issued.get("committed_receipt")
            if previous is not None:
                if not isinstance(previous, dict) or not isinstance(previous.get("execution"), dict):
                    raise SimulationRuntimeError("SIGNED_REUSED_EXECUTION_DIFFERS")
                verify_ack(previous, {"snapshot_id": snapshot_id, "result": result,
                                      "execution": previous["execution"]})
                rows = result.get("strategy_rows", []) + result.get("combination_rows", [])
                if strategy_key:
                    rows = [row for row in rows if row["strategy_key"] == strategy_key]
                complete_input_request(input_path)
                return {
                    "schema": "probiga.qmt-strategy-worker-receipt.v1", "status": "reused",
                    "trade_date": result["trade_date"], "snapshot_id": snapshot_id,
                    "run_uid": previous["run_uid"], "result_hash": previous["result_hash"],
                    "simulation_only": True, "automatic_real_order_submission": False,
                    "original_execution": previous["execution"], "rows": rows,
                }
            # Prove QMT stayed at the same frozen native model during evaluation.
            final_identity = verify_bridge_identity(read_json(paths["heartbeat"]), build_sha)
            if final_identity["model_instance_id"] != identity["model_instance_id"]:
                raise SimulationRuntimeError("QMT_MODEL_CHANGED_DURING_EXECUTION")
            payload = {
                "schema": COMMIT_SCHEMA, "edge_build_sha": build_sha,
                "snapshot_id": snapshot_id, "result": result,
                "execution": {"origin": origin, "started_at": started_at,
                              "finished_at": now_text(), "bridge_identity": final_identity},
            }
            retained = state_root / (snapshot_id + ".pending.json")
            atomic_json(retained, payload)
            receipts = publish_retained(client, state_root, build_sha)
            receipt = next((item for item in receipts if item.get("snapshot_id") == snapshot_id), None)
            if receipt is None:
                raise SimulationRuntimeError("RESULT_PUBLICATION_NOT_ACKNOWLEDGED")
            complete_input_request(input_path)
            rows = result.get("strategy_rows", []) + result.get("combination_rows", [])
            if strategy_key:
                rows = [row for row in rows if row.get("strategy_key") == strategy_key]
                if not rows:
                    raise SimulationRuntimeError("EXCLUDED_OR_UNKNOWN_STRATEGY_KEY")
            return {
                "schema": "probiga.qmt-strategy-worker-receipt.v1",
                "status": "completed", "trade_date": result["trade_date"],
                "snapshot_id": snapshot_id, "run_uid": receipt.get("run_uid"),
                "result_hash": receipt["result_hash"], "simulation_only": True,
                "automatic_real_order_submission": False, "rows": rows,
            }
    finally:
        client.close()


def main(argv: list[str] | None = None) -> int:
    load_project_env(ROOT / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--expected-build-sha", default=None)
    parser.add_argument("--strategy-key", default="")
    parser.add_argument("--origin", choices=("WINDOWS_DAILY", "QMT_ENTRY"), default="WINDOWS_DAILY")
    parser.add_argument("--server-url", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    url = args.server_url or os.environ.get("PROBIGA_QMT_INGEST_SERVER_URL", "")
    if not url:
        from tools.remote_support import remote_host
        url = "http://" + remote_host()
    try:
        result = run(server_url=url.rstrip("/"), trade_date=args.trade_date,
                     expected_build_sha=args.expected_build_sha,
                     strategy_key=args.strategy_key, origin=args.origin)
    except Exception as exc:
        # SQL/transport exceptions can contain secrets; never print their raw text.
        result = {"schema": "probiga.qmt-strategy-worker-receipt.v1", "status": "error",
                  "error_type": type(exc).__name__,
                  "reason": str(exc) if isinstance(exc, SimulationRuntimeError) else "SIMULATION_EXECUTION_OR_UPLOAD_FAILED",
                  "simulation_only": True, "automatic_real_order_submission": False}
        print(json.dumps(result, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
