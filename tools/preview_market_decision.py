"""Freeze, generate, resume and accept real mutually reviewed market reports.

The only generation path is the production dual-model orchestrator. Publishing
is explicit, atomic and verified through the canonical production API. This
entry point has no response injection, curation or notification capability.
"""
from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import socket
import subprocess
from pathlib import Path
import sys
from threading import RLock
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.review.research_flow import artifact_hash, assert_digest, prepare_research_facts

BUNDLE_SCHEMA = "probiga.research-bundle.v1"
RUN_SCHEMA = "probiga.research-acceptance-run.v1"
MAX_BUNDLE_BYTES = 30_000_000
RUN_SECONDS = 1320
FACT_FIELDS = ("review_date", "adjust_type", "generated_at", "data_cutoff_at", "publish_status",
               "quality_json", "market_structure_json", "industry_rotation_json", "factor_validation_json")
ARTIFACT_KEYS = {"research", "market_decision", "early_briefing", "selection_review", "editorial_review"}
RECOVERY_SCHEMA = "probiga.research-recovery.v1"
RECOVERY_STAGES = ("codex_draft", "codex_review", "codex_final_review")


def _default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(type(value).__name__)


def _bytes(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False, default=_default).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic(path: Path, value, *, raw=False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    data = value if raw else _bytes(value)
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _read_json(path: Path) -> dict:
    if not path.is_file() or path.stat().st_size > MAX_BUNDLE_BYTES:
        raise RuntimeError("FAILED stage=input: missing or oversized checkpoint")
    value = json.loads(path.read_bytes().decode("utf-8-sig"))
    if not isinstance(value, dict):
        raise RuntimeError("FAILED stage=input: checkpoint must be an object")
    return value


def _aware(value: str) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise RuntimeError("FAILED stage=bundle: cutoff requires timezone")
    return result


def _facts_only(value: dict) -> dict:
    result = {key: deepcopy(value[key]) for key in FACT_FIELDS if key in value}
    result["compact_review"] = ""
    result["factor_validation_json"] = {key: item for key, item in result.get("factor_validation_json", {}).items()
                                        if key not in ARTIFACT_KEYS}
    return result


def freeze_bundle(facts: dict, review_context: dict, briefing_context: dict, *, next_trade_date: str,
                  previous_research=None, source_raw: dict[str, bytes] | None = None) -> dict:
    """Seal real inputs and their original bytes before any external call."""
    from biz.review.market_decision import source_digest_identity, verify_automatic_research
    source = {"facts": facts, "review_context": review_context, "briefing_context": briefing_context}
    raw = source_raw or {key: _bytes(value) for key, value in source.items()}
    receipts, blobs = {}, {}
    for key, value in source.items():
        data = raw[key]
        if not isinstance(data, bytes) or len(data) > MAX_BUNDLE_BYTES // 2 or json.loads(data.decode("utf-8")) != value:
            raise RuntimeError("FAILED stage=bundle: original bytes differ from " + key)
        receipts[key] = {"raw_sha256": _sha(data), "raw_size": len(data)}
        blobs[key] = base64.b64encode(data).decode("ascii")
    frozen_facts = prepare_research_facts(_facts_only(facts), review_context)
    target = str(frozen_facts.get("review_date", ""))[:10]
    assert_digest(frozen_facts, target)
    if date.fromisoformat(next_trade_date) <= date.fromisoformat(target):
        raise RuntimeError("FAILED stage=bundle: next session must follow review session")
    if previous_research is not None:
        verify_automatic_research(previous_research)
        if previous_research.get("next_trade_date") != target:
            raise RuntimeError("FAILED stage=bundle: previous research targets another session")
    frozen = {"schema_version": BUNDLE_SCHEMA, "frozen_at": datetime.now(timezone.utc).isoformat(),
              "review_date": target, "next_trade_date": next_trade_date, "facts": frozen_facts,
              "review_context": deepcopy(review_context), "briefing_context": deepcopy(briefing_context),
              "previous_research": deepcopy(previous_research), "source_receipts": receipts, "source_blobs": blobs,
              "source_digest_sha256": source_digest_identity(frozen_facts),
              "data_cutoff_at": frozen_facts.get("data_cutoff_at")}
    frozen["bundle_sha256"] = artifact_hash(frozen)
    validate_bundle(frozen)
    return frozen


def validate_bundle(bundle: dict) -> None:
    from biz.review.market_decision import source_digest_identity, verify_automatic_research
    if (bundle.get("schema_version") != BUNDLE_SCHEMA
            or bundle.get("bundle_sha256") != artifact_hash({key: value for key, value in bundle.items() if key != "bundle_sha256"})):
        raise RuntimeError("FAILED stage=bundle: manifest identity mismatch")
    frozen_at = _aware(bundle["frozen_at"])
    decoded = {}
    for key in ("facts", "review_context", "briefing_context"):
        raw = base64.b64decode(bundle["source_blobs"][key], validate=True)
        receipt = bundle["source_receipts"][key]
        if len(raw) > MAX_BUNDLE_BYTES // 2 or len(raw) != receipt["raw_size"] or _sha(raw) != receipt["raw_sha256"]:
            raise RuntimeError("FAILED stage=bundle: source bytes changed")
        decoded[key] = json.loads(raw.decode("utf-8"))
        if key != "facts" and decoded[key] != bundle[key]:
            raise RuntimeError("FAILED stage=bundle: frozen context differs from original bytes")
    expected_facts = prepare_research_facts(_facts_only(decoded["facts"]), bundle["review_context"])
    if expected_facts != bundle["facts"] or bundle.get("data_cutoff_at") != expected_facts.get("data_cutoff_at"):
        raise RuntimeError("FAILED stage=bundle: frozen facts differ from source")
    target = bundle["review_date"]
    assert_digest(bundle["facts"], target)
    if (date.fromisoformat(bundle["next_trade_date"]) <= date.fromisoformat(target)
            or source_digest_identity(bundle["facts"]) != bundle["source_digest_sha256"]):
        raise RuntimeError("FAILED stage=bundle: invalid session or source identity")
    for key in ("review_context", "briefing_context"):
        context = bundle[key]
        if _aware(context["cutoff_at"]) > frozen_at or _aware(context.get("market_data_asof_at", context["cutoff_at"])) > frozen_at:
            raise RuntimeError("FAILED stage=bundle: future context cutoff")
    previous = bundle.get("previous_research")
    if previous is not None:
        verify_automatic_research(previous)
        if previous.get("next_trade_date") != target:
            raise RuntimeError("FAILED stage=bundle: wrong previous hypothesis session")


def _source_ordered_contexts(bundle: dict) -> dict:
    """Restore the exact frozen context serialization, not just JSON equality.

    The fact packet's historical context hash uses its source key order. The
    run manifest is sorted for hashing, so restore from retained original bytes
    before rebuilding a packet. No value or recorded identity is rewritten.
    """
    result = deepcopy(bundle)
    for key in ("review_context", "briefing_context"):
        restored = json.loads(base64.b64decode(bundle["source_blobs"][key], validate=True).decode("utf-8"))
        if restored != bundle[key]:
            raise RuntimeError("BLOCKED stage=resume: original context content changed")
        result[key] = restored
    return result


def verify_api_payload(payload: dict, digest: dict, briefing: dict) -> dict:
    from biz.review.research_flow import decode_digest, verify_report_pair
    target = digest["review_date"]
    if payload.get("date") != target or payload.get("fallback") is not False or len(payload.get("data", [])) != 1:
        raise RuntimeError("FAILED stage=production_api: exact report missing or fallback used")
    stored = decode_digest(payload["data"][0])
    morning = stored["factor_validation_json"].get("early_briefing") or {}
    verify_report_pair(stored, morning)
    if (stored.get("compact_review") != digest["compact_review"] or morning != briefing
            or stored["factor_validation_json"]["market_decision"] != digest["factor_validation_json"]["market_decision"]):
        raise RuntimeError("FAILED stage=production_api: published artifacts differ from untouched generation")
    return {"status": "COMPLETE", "review_date": target, "briefing_date": briefing["next_trade_date"],
            "fallback": False, "review_body_sha256": _sha(stored["compact_review"].encode("utf-8")),
            "briefing_body_sha256": _sha(morning["content"].encode("utf-8")),
            "review_sha256": stored["factor_validation_json"]["market_decision"]["research_sha256"],
            "briefing_sha256": morning["research_sha256"], "wecom_delivery_invoked": False}


def verify_production_api(base_url: str, digest: dict, briefing: dict) -> tuple[dict, dict]:
    import asyncio
    import httpx
    async def request():
        async with asyncio.timeout(15):
            async with httpx.AsyncClient(timeout=httpx.Timeout(12, connect=5), trust_env=False,
                                         transport=httpx.AsyncHTTPTransport(retries=0)) as client:
                response = await client.get(base_url.rstrip("/") + "/api/hot-data/daily-review/quant",
                                            params={"review_date": digest["review_date"]})
                response.raise_for_status()
                if len(response.content) > MAX_BUNDLE_BYTES:
                    raise RuntimeError("FAILED stage=production_api: response budget exceeded")
                return response.json()
    payload = asyncio.run(request())
    return payload, verify_api_payload(payload, digest, briefing)


def _pid_absent(pid: int) -> bool:
    """Prove absence, without opening another process's environment or memory."""
    if type(pid) is not int or pid <= 0:
        raise RuntimeError("BLOCKED stage=recovery_pid: invalid process identity")
    if os.name == "nt":
        import ctypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = (ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.CloseHandle.argtypes = (ctypes.c_void_p,)
        handle = kernel.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION only.
        if handle:
            kernel.CloseHandle(handle)
            return False
        if ctypes.get_last_error() == 87:  # ERROR_INVALID_PARAMETER: PID does not exist.
            return True
        raise RuntimeError("BLOCKED stage=recovery_pid: process absence cannot be verified")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError as exc:
        raise RuntimeError("BLOCKED stage=recovery_pid: process access denied") from exc
    return False


def _prior_worker_identity(revision: str) -> dict:
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise RuntimeError("BLOCKED stage=recovery_worker: exact prior merged revision required")
    ancestry = subprocess.run(["git", "-C", str(ROOT), "merge-base", "--is-ancestor", revision,
                               "refs/remotes/origin/main"], capture_output=True, timeout=15, check=False)
    if ancestry.returncode:
        raise RuntimeError("BLOCKED stage=recovery_worker: prior revision is not merged origin/main history")
    path = "tools/run_codex_web_bridge.py"
    result = subprocess.run(["git", "-C", str(ROOT), "show", revision + ":" + path],
                            capture_output=True, timeout=15, check=False)
    if result.returncode or not result.stdout or len(result.stdout) > 200_000:
        raise RuntimeError("BLOCKED stage=recovery_worker: prior worker blob unavailable")
    return {"revision": revision, "path": path, "sha256": _sha(result.stdout), "bytes": len(result.stdout)}


def _canonical_recovery_row(engine, request_id: str) -> dict:
    from server.ai_bridge import research
    from sqlalchemy import text
    bounded = research.get_research_engine(engine)
    row = research._load(bounded, request_id)
    if not row or row["status"] != "failed" or row["channel"] != "research":
        raise RuntimeError("BLOCKED stage=recovery_database: original request is not canonically failed")
    with bounded.connect() as connection:
        active = connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ request_uid FROM st_ai_bridge_job "
                                          "WHERE channel='research' AND status IN ('queued','processing') LIMIT 1")).first()
    if active:
        raise RuntimeError("BLOCKED stage=recovery_database: another research job is active")
    return row


def _recovery_request(core, packet: dict, rules: str, checkpoint: dict, stage: str):
    stages = checkpoint["stages"]
    if stage == "codex_draft":
        return core._draft_request(packet, rules)
    if stage == "codex_review":
        return core._peer_request(packet, rules, stages["deepseek_draft"]["raw_response"])
    reviews = {"deepseek_review": core._parse_review(stages["deepseek_review"]["raw_response"],
                    core._body_hash(stages["codex_draft"]["raw_response"]), packet),
               "codex_review": core._parse_review(stages["codex_review"]["raw_response"],
                    core._body_hash(stages["deepseek_draft"]["raw_response"]), packet)}
    final = core._validate_final(stages["deepseek_final"]["raw_response"], packet, reviews)
    return core._audit_request(packet, rules, final, reviews)


def inspect_recovery(engine, source_dir: Path, *, stage: str, worker_state_root: Path,
                     registration_path: Path, prior_worker_revision: str) -> dict:
    """Read-only preflight distinguishing transport failures from rejected work."""
    from biz.review import market_decision as core
    from biz.review.research_flow import publication_snapshot
    from server.ai_bridge.research import decode_envelope, validate_payload
    import time
    stop = time.monotonic() + 120
    def require(condition, reason):
        if time.monotonic() >= stop:
            raise RuntimeError("BLOCKED stage=recovery: proof_deadline_exceeded")
        if not condition:
            raise RuntimeError("BLOCKED stage=recovery: " + reason)
    source_dir, worker_state_root = source_dir.resolve(), worker_state_root.resolve()
    require(stage in RECOVERY_STAGES, "only_codex_transport_stages_are_recoverable")
    require(not (source_dir / "recovery_claim.json").exists(), "source_already_claimed_for_recovery")
    original = _read_json(source_dir / "run_receipt.json")
    require(original.get("run_id") == source_dir.name and original.get("status") == "FAILED", "original_run_not_failed")
    require(not original.get("recovery") and not original.get("recovery_of"), "recovery_limit_reached")
    require(original.get("report_database_write_invoked") is False and original.get("wecom_delivery_invoked") is False,
            "original_run_crossed_publication_boundary")
    bundle = _read_json(source_dir / "bundle.json")
    validate_bundle(bundle)
    bundle = _source_ordered_contexts(bundle)
    require(original.get("bundle_sha256") == bundle["bundle_sha256"], "source_bundle_identity_changed")
    rules, rules_hash = core._load_rules()
    require(original.get("rules_sha256") == rules_hash, "rules_changed")
    candidates = []
    for kind in ("review", "briefing"):
        path = source_dir / (kind + "_checkpoint.json")
        if path.exists():
            value = _read_json(path)
            if value.get("stages", {}).get(stage, {}).get("status") == "FAILED":
                candidates.append((kind, value))
    require(len(candidates) == 1, "exact_failed_stage_checkpoint_required")
    kind, checkpoint = candidates[0]
    require(not checkpoint.get("recovery"), "recovery_limit_reached")
    require(checkpoint.get("rules_sha256") == rules_hash, "checkpoint_rules_changed")
    require(set(checkpoint["stages"]).issubset(core.STAGES), "unknown_stage")
    require(all(item.get("status") == "COMPLETE" or name == stage and item.get("status") == "FAILED"
                for name, item in checkpoint["stages"].items()), "another_stage_is_not_terminal")
    require(_aware(checkpoint["generation_started_at"]) == _aware(original["report_started_at"][kind]), "generation_clock_changed")
    digest = _read_json(source_dir / "digest.json") if kind == "briefing" else deepcopy(bundle["facts"])
    previous = bundle.get("previous_research")
    if kind == "briefing":
        previous = digest["factor_validation_json"]["market_decision"]
        core.verify_automatic_research(previous)
    packet = core.build_packet(digest, report_kind=kind, next_trade_date=bundle["next_trade_date"],
        context=bundle[kind + "_context"], previous_research=previous,
        now=core._now(_aware(checkpoint["generation_started_at"])), source_digest_sha256=core.source_digest_identity(digest))
    require(core._hash(packet) == checkpoint.get("input_sha256"), "frozen_packet_changed")
    messages, schema = _recovery_request(core, packet, rules, checkpoint, stage)
    prior = checkpoint["stages"][stage]
    request_id = prior.get("request_id")
    try:
        valid_uuid = isinstance(request_id, str) and str(UUID(request_id)) == request_id
    except (ValueError, TypeError):
        valid_uuid = False
    require(valid_uuid, "prior_attempt_uuid_invalid")
    failed_input = _read_json(source_dir / "stages" / kind / (stage + "_input.json"))
    require(failed_input == {"messages": messages, "response_schema": schema}
            and prior.get("messages_sha256") == core._hash(messages), "messages_or_schema_changed")
    for name, item in checkpoint["stages"].items():
        if item.get("status") == "COMPLETE":
            raw = (source_dir / "stages" / kind / (name + "_response.json")).read_bytes()
            receipt = item.get("provider_receipt") or {}
            require(raw.decode("utf-8") == item.get("raw_response") and _sha(raw) == item.get("raw_sha256")
                    and receipt.get("raw_sha256") == item.get("raw_sha256")
                    and receipt.get("messages_sha256") == item.get("messages_sha256")
                    and receipt.get("transport") == ("live_http" if name.startswith("deepseek") else "live_codex_bridge"),
                    "successful_stage_raw_or_provider_receipt_changed")
            if name.startswith("deepseek"):
                require(receipt.get("source") == "deepseek_api"
                        and receipt.get("endpoint") == "https://api.deepseek.com/v1/chat/completions"
                        and receipt.get("provider_response_id") and receipt.get("model"), "successful_provider_identity_missing")
            else:
                require(receipt.get("source") == "codex_gpt" and receipt.get("request_id") == item.get("request_id")
                        and receipt.get("rules_sha256") == rules_hash and receipt.get("ephemeral") is True
                        and receipt.get("process_exit_code") == 0 and receipt.get("session_id"), "successful_provider_identity_missing")
    row = _canonical_recovery_row(engine, request_id)
    payload = validate_payload(decode_envelope(row["question"]))
    require(payload["request_id"] == request_id and payload["stage"] == stage
            and payload["messages"] == messages and payload["response_schema"] == schema
            and payload["rules_sha256"] == rules_hash, "canonical_failed_input_changed")
    require(str(row.get("error_message") or "") in {"request_total_deadline_exceeded", "processing_request_deadline_exceeded"}
            or "network" in str(row.get("error_message") or "").lower(), "canonical_failure_is_not_transport")
    worker_dir = worker_state_root / request_id
    local_path, process_path = worker_dir / "receipt.json", worker_dir / "process" / "receipt.json"
    local, process = _read_json(local_path), _read_json(process_path)
    owner, generation = row.get("worker_id"), row.get("attempts")
    require(local.get("worker_id") == owner and local.get("claim_generation") == generation
            and local.get("request_id") == request_id and local.get("input_sha256") == payload["input_sha256"], "local_owner_generation_or_input_mismatch")
    require(process.get("status") == "TERMINAL" and type(process.get("process_exit_code")) is int
            and process["process_exit_code"] != 0 and process.get("request_id") == request_id
            and process.get("input_sha256") == payload["input_sha256"], "prior_child_terminal_failure_unproved")
    completion = local.get("completion") or {}
    receipt = completion.get("provider_receipt") or {}
    require(completion.get("status") == "failed" and completion.get("worker_id") == owner
            and completion.get("claim_generation") == generation and receipt.get("source") == "codex_gpt"
            and receipt.get("request_id") == request_id and receipt.get("input_sha256") == payload["input_sha256"]
            and receipt.get("rules_sha256") == rules_hash,
            "local_failure_receipt_mismatch")
    prefix = "research-" + socket.gethostname() + "-"
    require(isinstance(owner, str) and owner.lower().startswith(prefix.lower()), "owner_is_not_this_windows_host")
    parent_pid = owner[len(prefix):].split("-", 1)[0]
    require(parent_pid.isdigit() and process.get("parent_pid") == int(parent_pid), "prior_parent_process_mismatch")
    require(_pid_absent(process["pid"]), "prior_child_pid_still_exists")
    answer_path = worker_dir / "answer.json"
    require(not answer_path.exists() or answer_path.stat().st_size == 0, "prior_attempt_produced_model_output")
    events_path = worker_dir / "events.jsonl"
    errors = []
    for line in events_path.read_bytes().splitlines():
        event = json.loads(line)
        item = event.get("item") or {}
        require(not (item.get("type") == "agent_message" and item.get("text")), "prior_attempt_produced_model_output")
        if event.get("type") == "error":
            errors.append(str(event.get("message") or ""))
    text = " ".join(errors).lower()
    require(any(word in text for word in ("network", "error sending request", "connection failed", "connection timed out", "stream disconnected"))
            and not any(word in text for word in ("schema", "bad request", "unauthorized", "forbidden", "invalid", "rejected", "400", "401", "403")),
            "transport_error_evidence_required_not_semantic_rejection")
    registration = _read_json(registration_path)
    registered = registration.get("registration") or {}
    prior_worker = _prior_worker_identity(prior_worker_revision)
    require(registration.get("status") == "REGISTERED" and registered.get("purpose") == "research"
            and registered.get("task_name") == "ProBigA AI Research Worker"
            and Path(registered["state_root"]).resolve() == worker_state_root
            and registered.get("worker_sha256") == process.get("worker_source_sha256") == prior_worker["sha256"],
            "original_registered_worker_identity_unproved")
    guard = original.get("publication_guard")
    if original.get("publish"):
        current = publication_snapshot(engine, bundle["review_date"])
        require(guard and all(current.get(key) == guard.get(key) for key in ("row_sha256", "source_digest_sha256")), "publication_source_changed")
    database_identity = {key: row.get(key) for key in ("request_uid", "channel", "status", "worker_id", "attempts",
        "source", "provider_attempt", "error_message", "created_at", "started_at", "completed_at", "updated_at")}
    database_identity.update(question_sha256=_sha(row["question"].encode("utf-8")), input_sha256=payload["input_sha256"],
                             rules_sha256=rules_hash)
    proof = {"database_row": database_identity, "database_row_sha256": artifact_hash(row),
             "local_receipt": local, "local_receipt_raw_sha256": _sha(local_path.read_bytes()),
             "process_receipt": process, "process_receipt_raw_sha256": _sha(process_path.read_bytes()),
             "events_raw_sha256": _sha(events_path.read_bytes()), "network_error_observed": True,
             "pid_absence": {"pid": process["pid"], "absent": True, "host": socket.gethostname()},
             "registration_receipt": registration, "registration_raw_sha256": _sha(registration_path.read_bytes()),
             "prior_worker_git": prior_worker, "messages_sha256": core._hash(messages), "schema_sha256": core._hash(schema),
             "input_sha256": payload["input_sha256"], "packet_sha256": checkpoint["input_sha256"],
             "verified_at": datetime.now(timezone.utc).isoformat()}
    source_files = {}
    names = ["bundle.json", "run_receipt.json", "review_checkpoint.json", "briefing_checkpoint.json", "digest.json", "review.md"]
    for report_kind in ("review", "briefing"):
        for name in core.STAGES:
            names += [f"stages/{report_kind}/{name}_input.json", f"stages/{report_kind}/{name}_response.json"]
    for name in names:
        path = source_dir / name
        if path.exists():
            require(path.is_file() and path.stat().st_size <= MAX_BUNDLE_BYTES, "invalid_source_artifact")
            source_files[name] = _sha(path.read_bytes())
    return {"source_directory": source_dir, "source_run": original, "bundle": bundle, "kind": kind,
            "checkpoint": checkpoint, "prior_attempt": deepcopy(prior), "proof": proof, "source_files": source_files}


def prepare_recovery(engine, source_dir: Path, *, output_dir: Path, stage: str, worker_state_root: Path,
                     registration_path: Path, prior_worker_revision: str) -> Path:
    """Claim exactly one new bounded operation; never requests a model itself."""
    evidence = inspect_recovery(engine, source_dir, stage=stage, worker_state_root=worker_state_root,
                                registration_path=registration_path, prior_worker_revision=prior_worker_revision)
    source_dir = evidence["source_directory"]
    original, checkpoint = evidence["source_run"], deepcopy(evidence["checkpoint"])
    now, operation_id, request_id = datetime.now(timezone.utc), str(uuid4()), str(uuid4())
    recovery = {"schema": RECOVERY_SCHEMA, "recovery_of": original["run_id"], "failed_stage": stage,
                "prior_attempt": evidence["prior_attempt"], "proof": evidence["proof"],
                "request_ids": {stage: request_id}, "recovery_count": 1, "created_at": now.isoformat(),
                "prior_deadline_at": checkpoint["deadline_at"], "prior_run_deadline_at": original["deadline_at"]}
    directory = output_dir.resolve() / operation_id
    claim = {"schema": RECOVERY_SCHEMA, "recovery_of": original["run_id"], "operation_id": operation_id,
             "operation_directory": str(directory), "stage": stage, "new_request_id": request_id,
             "recovery_count": 1, "proof_sha256": artifact_hash(evidence["proof"]), "claimed_at": now.isoformat()}
    # Exclusive creation is the one-way recovery fence. A second operator or a
    # failed recovery cannot allocate another UUID from the original run.
    with (source_dir / "recovery_claim.json").open("xb") as handle:
        handle.write(_bytes(claim)); handle.flush(); os.fsync(handle.fileno())
    directory.mkdir(parents=True, exist_ok=False)
    for name, expected_hash in evidence["source_files"].items():
        raw = (source_dir / name).read_bytes()
        if _sha(raw) != expected_hash:
            raise RuntimeError("BLOCKED stage=recovery_copy: source artifact changed during preparation")
        if name != "run_receipt.json":
            _atomic(directory / name, raw, raw=True)
    _atomic(directory / "recovery" / "source_run_receipt.json", original)
    _atomic(directory / "recovery" / "source_checkpoint.json", evidence["checkpoint"])
    _atomic(directory / "recovery" / "proof.json", evidence["proof"])
    _atomic(directory / "recovery" / "manifest.json", recovery)
    checkpoint["recovery"] = recovery
    checkpoint["deadline_at"] = (now + timedelta(seconds=600)).isoformat()
    del checkpoint["stages"][stage]
    _atomic(directory / (evidence["kind"] + "_checkpoint.json"), checkpoint)
    receipt = deepcopy(original)
    for key in ("error", "error_type", "failed_at", "completed_at"):
        receipt.pop(key, None)
    receipt.update(run_id=operation_id, status="RUNNING", stage="recovery_prepared", recovery_of=original["run_id"],
        recovery=recovery, started_at=now.isoformat(), deadline_at=(now + timedelta(seconds=RUN_SECONDS)).isoformat(),
        report_database_write_invoked=False, wecom_delivery_invoked=False)
    _atomic(directory / "run_receipt.json", receipt)
    print(f"RUNNING recovery_of={original['run_id']} run_id={operation_id} stage={stage} directory={directory}", flush=True)
    return directory


def prepare_continuation(engine, source_dir: Path, *, output_dir: Path) -> Path:
    """An operator resumes one expired, fully completed phase boundary once."""
    from biz.review import market_decision as core
    from biz.review.research_flow import publication_snapshot
    from server.ai_bridge import research
    from sqlalchemy import text
    import time
    proof_deadline = time.monotonic() + 120
    source_dir = source_dir.resolve()
    def require(value, reason):
        if time.monotonic() >= proof_deadline:
            raise RuntimeError("BLOCKED stage=continuation: proof_deadline_exceeded")
        if not value:
            raise RuntimeError("BLOCKED stage=continuation: " + reason)
    require(not any((source_dir / name).exists() for name in ("continuation_claim.json", "recovery_claim.json")), "source_operation_already_claimed")
    original = _read_json(source_dir / "run_receipt.json")
    require(original.get("status") == "FAILED" and original.get("run_id") == source_dir.name, "source_not_terminal_failed")
    require(not original.get("continuation") and original.get("report_database_write_invoked") is False
            and original.get("wecom_delivery_invoked") is False, "continuation_limit_or_publication_boundary")
    require("deadline" in str(original.get("error", "")).lower(), "source_failure_is_not_expired_boundary")
    bundle = _read_json(source_dir / "bundle.json")
    validate_bundle(bundle)
    bundle = _source_ordered_contexts(bundle)
    rules, rules_hash = core._load_rules()
    require(original.get("bundle_sha256") == bundle["bundle_sha256"] and original.get("rules_sha256") == rules_hash, "bundle_or_rules_changed")
    now = datetime.now(timezone.utc)
    eligible = []
    for kind in ("review", "briefing"):
        path = source_dir / (kind + "_checkpoint.json")
        if path.exists():
            saved = _read_json(path)
            names = set(saved.get("stages", {}))
            if names in (set(core.STAGES[:2]), set(core.STAGES[:4])):
                eligible.append((kind, saved))
    require(len(eligible) == 1, "exact_two_or_four_stage_boundary_required")
    kind, checkpoint = eligible[0]
    require(not checkpoint.get("continuation") and all(item.get("status") == "COMPLETE" for item in checkpoint["stages"].values()), "noncomplete_or_already_continued_stage")
    require(_aware(checkpoint["deadline_at"]) <= now and checkpoint.get("rules_sha256") == rules_hash, "report_not_expired_or_rules_changed")
    require(_aware(checkpoint["generation_started_at"]) == _aware(original["report_started_at"][kind]), "generation_clock_changed")
    for name in set(core.STAGES) - set(checkpoint["stages"]):
        require(not any((source_dir / "stages" / kind / (name + "_" + suffix + ".json")).exists()
                        for suffix in ("input", "response")), "unrecorded_provider_outcome")
    digest = _read_json(source_dir / "digest.json") if kind == "briefing" else deepcopy(bundle["facts"])
    previous = digest["factor_validation_json"]["market_decision"] if kind == "briefing" else bundle.get("previous_research")
    if kind == "briefing":
        core.verify_automatic_research(previous)
    packet = core.build_packet(digest, report_kind=kind, next_trade_date=bundle["next_trade_date"], context=bundle[kind + "_context"],
        previous_research=previous, now=core._now(_aware(checkpoint["generation_started_at"])), source_digest_sha256=core.source_digest_identity(digest))
    require(core._hash(packet) == checkpoint.get("input_sha256"), "packet_changed")
    bounded = research.get_research_engine(engine)
    with bounded.connect() as connection:
        active = connection.execute(text("SELECT /*+ MAX_EXECUTION_TIME(15000) */ request_uid FROM st_ai_bridge_job WHERE channel='research' AND status IN ('queued','processing') LIMIT 1")).first()
    require(active is None, "another_research_job_is_active")
    for stage, item in checkpoint["stages"].items():
        if stage.endswith("_draft"):
            messages, schema = core._draft_request(packet, rules)
        else:
            other = "codex_draft" if stage == "deepseek_review" else "deepseek_draft"
            messages, schema = core._peer_request(packet, rules, checkpoint["stages"][other]["raw_response"])
        retained = _read_json(source_dir / "stages" / kind / (stage + "_input.json"))
        raw = (source_dir / "stages" / kind / (stage + "_response.json")).read_bytes()
        receipt = item.get("provider_receipt") or {}
        require(retained == {"messages": messages, "response_schema": schema} and core._hash(messages) == item.get("messages_sha256")
                and raw.decode("utf-8") == item.get("raw_response") and _sha(raw) == item.get("raw_sha256")
                and receipt.get("raw_sha256") == item.get("raw_sha256") and receipt.get("messages_sha256") == item.get("messages_sha256"), "saved_messages_schema_raw_or_receipt_changed")
        if stage.startswith("codex"):
            row = research._load(bounded, item["request_id"])
            require(row is not None and row["status"] == "completed", "canonical_codex_stage_not_complete")
            result = research._result(row)
            require(result.get("raw_response") == item["raw_response"] and receipt.get("transport") == "live_codex_bridge"
                    and receipt.get("process_exit_code") == 0 and receipt.get("ephemeral") is True
                    and all(receipt.get(key) == value for key, value in result.get("provider_receipt", {}).items()), "codex_completion_receipt_changed")
        else:
            require(receipt.get("source") == "deepseek_api" and receipt.get("transport") == "live_http"
                    and receipt.get("endpoint") == "https://api.deepseek.com/v1/chat/completions"
                    and receipt.get("provider_response_id") and receipt.get("model"), "deepseek_identity_missing")
        if stage.endswith("_review"):
            core._parse_review(item["raw_response"], schema["properties"]["reviewed_sha256"]["const"], packet)
    if original.get("publish"):
        guard = publication_snapshot(engine, bundle["review_date"])
        require(all(guard.get(key) == original["publication_guard"].get(key) for key in ("row_sha256", "source_digest_sha256")), "publication_source_changed")
    operation_id = str(uuid4())
    directory = output_dir.resolve() / operation_id
    continuation = {"schema": "probiga.research-continuation.v1", "continuation_of": original["run_id"], "continuation_count": 1,
        "prior_deadline_at": checkpoint["deadline_at"], "prior_run_deadline_at": original["deadline_at"],
        "old_receipt_sha256": _sha((source_dir / "run_receipt.json").read_bytes()),
        "old_checkpoint_sha256": _sha((source_dir / (kind + "_checkpoint.json")).read_bytes()),
        "created_at": now.isoformat(), "completed_stages": list(checkpoint["stages"])}
    claim = {**continuation, "operation_id": operation_id, "operation_directory": str(directory)}
    with (source_dir / "continuation_claim.json").open("xb") as handle:
        handle.write(_bytes(claim)); handle.flush(); os.fsync(handle.fileno())
    directory.mkdir(parents=True, exist_ok=False)
    names = ["bundle.json", "digest.json", "review.md", "review_checkpoint.json", "briefing_checkpoint.json"]
    for report_kind in ("review", "briefing"):
        for stage in core.STAGES:
            names.extend(f"stages/{report_kind}/{stage}_{suffix}.json" for suffix in ("input", "response"))
    for name in names:
        path = source_dir / name
        if path.is_file():
            _atomic(directory / name, path.read_bytes(), raw=True)
    _atomic(directory / "continuation/source_run_receipt.json", original)
    _atomic(directory / "continuation/source_checkpoint.json", checkpoint)
    _atomic(directory / "continuation/manifest.json", continuation)
    checkpoint["continuation"] = continuation
    checkpoint["deadline_at"] = (now + timedelta(seconds=600)).isoformat()
    _atomic(directory / (kind + "_checkpoint.json"), checkpoint)
    receipt = deepcopy(original)
    for key in ("error", "error_type", "failed_at", "completed_at"):
        receipt.pop(key, None)
    receipt.update(run_id=operation_id, status="RUNNING", stage="continuation_prepared", continuation=continuation,
        started_at=now.isoformat(), deadline_at=(now + timedelta(seconds=RUN_SECONDS)).isoformat())
    _atomic(directory / "run_receipt.json", receipt)
    print(f"RUNNING continuation_of={original['run_id']} run_id={operation_id} directory={directory}", flush=True)
    return directory


def execute_run(engine, bundle: dict, directory: Path, *, publish=False, api_base=None, resume=False) -> dict:
    from biz.review.market_decision import DEADLINE_SECONDS, rules_identity, source_digest_identity, verify_automatic_research
    from biz.review.research_flow import attach_review, generate_briefing, publication_snapshot, persist_report_pair, verify_report_pair, load_digest
    validate_bundle(bundle)
    bundle = _source_ordered_contexts(bundle)
    receipt_path = directory / "run_receipt.json"
    rule_hash = rules_identity()
    if resume:
        receipt = _read_json(receipt_path)
        if (receipt.get("bundle_sha256") != bundle["bundle_sha256"] or receipt.get("rules_sha256") != rule_hash
                or receipt.get("publish") != publish or receipt.get("api_base") != api_base):
            raise RuntimeError("FAILED stage=resume: frozen input, rules or publication scope changed")
        UUID(receipt["run_id"])
    else:
        if receipt_path.exists():
            raise RuntimeError("FAILED stage=run_identity: existing run requires explicit resume")
        directory.mkdir(parents=True, exist_ok=False)
        _atomic(directory / "bundle.json", bundle)
        now = datetime.now(timezone.utc)
        receipt = {"schema_version": RUN_SCHEMA, "run_id": directory.name, "bundle_sha256": bundle["bundle_sha256"],
                   "rules_sha256": rule_hash, "status": "RUNNING", "stage": "freeze", "started_at": now.isoformat(),
                   "deadline_at": (now + timedelta(seconds=RUN_SECONDS)).isoformat(), "total_units": 14 if publish else 12,
                   "completed_units": 0, "report_started_at": {}, "publish": publish, "api_base": api_base,
                   "publication_guard": publication_snapshot(engine, bundle["review_date"]) if publish else None,
                   "report_database_write_invoked": False, "wecom_delivery_invoked": False}
        UUID(receipt["run_id"])
        _atomic(receipt_path, receipt)
    lock = RLock()
    def gate(seconds=0):
        if (datetime.now(timezone.utc) + timedelta(seconds=seconds)) >= _aware(receipt["deadline_at"]):
            raise RuntimeError("FAILED stage=" + receipt.get("stage", "run") + ": total_run_deadline")
    def progress(kind):
        def record(event):
            with lock:
                gate()
                stage = str(event.get("stage", "unknown"))
                if not stage.replace("_", "").isalnum():
                    raise RuntimeError("FAILED stage=checkpoint: invalid stage name")
                if "checkpoint" in event:
                    _atomic(directory / (kind + "_checkpoint.json"), event["checkpoint"])
                    for name, item in event["checkpoint"].get("stages", {}).items():
                        if item.get("status") == "COMPLETE":
                            raw = item["raw_response"].encode("utf-8")
                            if _sha(raw) != item["raw_sha256"]:
                                raise RuntimeError("FAILED stage=checkpoint: raw response hash mismatch")
                            _atomic(directory / "stages" / kind / (name + "_response.json"), raw, raw=True)
                if event.get("messages") is not None:
                    _atomic(directory / "stages" / kind / (stage + "_input.json"),
                            {"messages": event["messages"], "response_schema": event.get("response_schema")})
                receipt.update(status="RUNNING", stage=kind + ":" + stage,
                               last_progress_at=event.get("last_progress_at", datetime.now(timezone.utc).isoformat()))
                completed = event.get("completed")
                if isinstance(completed, int):
                    receipt["completed_units"] = (6 if kind == "briefing" else 0) + completed
                _atomic(receipt_path, receipt)
                print(f"RUNNING run_id={receipt['run_id']} stage={receipt['stage']} completed={receipt['completed_units']} total={receipt['total_units']}", flush=True)
        return record
    def checkpoint(kind):
        path = directory / (kind + "_checkpoint.json")
        return _read_json(path) if path.exists() else None
    def generation_time(kind):
        if kind not in receipt["report_started_at"]:
            receipt["report_started_at"][kind] = datetime.now(timezone.utc).isoformat()
            _atomic(receipt_path, receipt)
        return _aware(receipt["report_started_at"][kind])
    digest = deepcopy(bundle["facts"])
    kind = "review"
    try:
        digest_path = directory / "digest.json"
        if digest_path.exists():
            digest = _read_json(digest_path)
            parent = digest["factor_validation_json"]["market_decision"]
            verify_automatic_research(parent)
            if parent.get("source_digest_sha256") != source_digest_identity(bundle["facts"]):
                raise RuntimeError("FAILED stage=resume: review source identity changed")
        else:
            gate(DEADLINE_SECONDS)
            attach_review(engine, digest, context=bundle["review_context"], next_trade_date=bundle["next_trade_date"],
                          previous_research=bundle.get("previous_research"), now=generation_time("review"),
                          checkpoint=checkpoint("review"), progress=progress("review"))
            _atomic(digest_path, digest)
            _atomic(directory / "review.md", digest["compact_review"].encode("utf-8"), raw=True)
        kind = "briefing"
        briefing_path = directory / "briefing.json"
        if briefing_path.exists():
            briefing = _read_json(briefing_path)
            verify_report_pair(digest, briefing)
        else:
            gate(DEADLINE_SECONDS)
            briefing = generate_briefing(engine, bundle["review_date"], persist=False, digest=digest,
                context=bundle["briefing_context"], next_trade_date=bundle["next_trade_date"], now=generation_time("briefing"),
                checkpoint=checkpoint("briefing"), progress=progress("briefing"))
            verify_report_pair(digest, briefing)
            _atomic(briefing_path, briefing)
            _atomic(directory / "briefing.md", briefing["content"].encode("utf-8"), raw=True)
        receipt.update(completed_units=12, stage="generation_complete", status="GENERATED")
        _atomic(receipt_path, receipt)
        if publish:
            if not api_base:
                raise RuntimeError("FAILED stage=publication: --api-base required for end-to-end acceptance")
            gate(75)
            publication_path = directory / "publication_receipt.json"
            if publication_path.exists():
                published = load_digest(engine, bundle["review_date"])
                verify_report_pair(published, published["factor_validation_json"]["early_briefing"])
                if (published["factor_validation_json"]["market_decision"] != digest["factor_validation_json"]["market_decision"]
                        or published["factor_validation_json"]["early_briefing"] != briefing):
                    raise RuntimeError("FAILED stage=publication_resume: durable report differs")
            else:
                receipt.update(stage="publication", report_database_write_invoked=True)
                _atomic(receipt_path, receipt)
                publication = persist_report_pair(engine, digest, briefing, expected=receipt["publication_guard"],
                    backup=lambda value: _atomic(directory / "publication_before.json", value), progress=progress("publication"))
                _atomic(publication_path, publication)
            receipt.update(completed_units=13, stage="production_api")
            _atomic(receipt_path, receipt)
            payload, accepted = verify_production_api(api_base, digest, briefing)
            _atomic(directory / "production_api.json", payload)
            _atomic(directory / "acceptance_receipt.json", accepted)
            receipt["completed_units"] = 14
        receipt.update(status="COMPLETE", stage="acceptance", completed_at=datetime.now(timezone.utc).isoformat())
        _atomic(receipt_path, receipt)
        print(f"COMPLETE run_id={receipt['run_id']} directory={directory} published={str(publish).lower()} wecom=false", flush=True)
        return receipt
    except BaseException as exc:
        saved = getattr(exc, "checkpoint", None)
        if isinstance(saved, dict):
            _atomic(directory / (kind + "_checkpoint.json"), saved)
        receipt.update(status="FAILED", error_type=type(exc).__name__, error=str(exc)[:2000], failed_at=datetime.now(timezone.utc).isoformat())
        _atomic(receipt_path, receipt)
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, help="完整冻结包；原始事实与两个context均需有字节校验")
    parser.add_argument("--date", default="")
    parser.add_argument("--output-dir", type=Path, required=True, help="每次新建UUID子目录")
    parser.add_argument("--resume", type=Path, help="精确UUID运行目录；仅恢复已成功阶段，不重请求失败阶段")
    parser.add_argument("--recover-from", type=Path, help="显式为已核验终态的原FAILED研究创建一次新恢复操作")
    parser.add_argument("--recover-stage", choices=RECOVERY_STAGES)
    parser.add_argument("--worker-state-root", type=Path, default=Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "ProBigA" / "research-worker")
    parser.add_argument("--prior-worker-registration", type=Path, help="原失败worker的不可变注册回执副本")
    parser.add_argument("--prior-worker-revision", default="", help="原worker所属已合并main完整Git SHA")
    parser.add_argument("--publish", action="store_true", help="原子发布两份自动稿并完成生产API验收；不推送")
    parser.add_argument("--api-base", default="", help="发布时必填，例如 http://47.113.123.190")
    args = parser.parse_args(argv)
    if sum(bool(value) for value in (args.resume, args.bundle, args.recover_from)) > 1:
        parser.error("--resume, --bundle and --recover-from are mutually exclusive")
    if args.recover_from and not (args.recover_stage and args.prior_worker_registration and args.prior_worker_revision):
        parser.error("recovery requires --recover-stage, --prior-worker-registration and --prior-worker-revision")
    if args.recover_stage and not args.recover_from:
        parser.error("--recover-stage requires --recover-from")
    if args.publish and not args.api_base:
        parser.error("--publish requires --api-base")
    from server.common.batch_db import create_batch_engine
    engine = create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 15, "write_timeout": 15})
    try:
        recovering = bool(args.recover_from)
        if recovering:
            source = args.recover_from.resolve()
            prior = _read_json(source / "run_receipt.json")
            if prior.get("publish") != args.publish or prior.get("api_base") != (args.api_base.rstrip("/") or None):
                raise RuntimeError("BLOCKED stage=recovery: original_publication_scope_required")
            directory = prepare_recovery(engine, source, output_dir=args.output_dir.resolve(), stage=args.recover_stage,
                worker_state_root=args.worker_state_root, registration_path=args.prior_worker_registration,
                prior_worker_revision=args.prior_worker_revision)
            bundle = _read_json(directory / "bundle.json")
        elif args.resume:
            directory = args.resume.resolve()
            prior = _read_json(directory / "run_receipt.json")
            expired_boundary = False
            from biz.review.market_decision import STAGES
            for kind in ("review", "briefing"):
                path = directory / (kind + "_checkpoint.json")
                if path.exists():
                    state = _read_json(path)
                    if (set(state.get("stages", {})) in (set(STAGES[:2]), set(STAGES[:4]))
                            and _aware(state["deadline_at"]) <= datetime.now(timezone.utc)):
                        expired_boundary = True
            if prior.get("status") == "FAILED" and expired_boundary:
                if prior.get("publish") != args.publish or prior.get("api_base") != (args.api_base.rstrip("/") or None):
                    raise RuntimeError("BLOCKED stage=continuation: original_publication_scope_required")
                directory = prepare_continuation(engine, directory, output_dir=args.output_dir.resolve())
            bundle = _read_json(directory / "bundle.json")
        else:
            if args.bundle:
                bundle = _read_json(args.bundle.resolve())
            else:
                from biz.evening_review.generate import resolve_review_trade_date
                from biz.review.research_flow import load_digest, collect_context, automatic_previous_research, next_session
                target = resolve_review_trade_date(engine, args.date)
                facts = load_digest(engine, target)
                review = collect_context(engine, target)
                morning = collect_context(engine, target, overnight=True)
                bundle = freeze_bundle(facts, review, morning, next_trade_date=next_session(engine, target),
                                       previous_research=automatic_previous_research(engine, facts))
            args.output_dir.resolve().mkdir(parents=True, exist_ok=True)
            directory = args.output_dir.resolve() / str(uuid4())
        if args.date and args.date != bundle["review_date"]:
            raise RuntimeError("FAILED stage=input: date differs from frozen bundle")
        execute_run(engine, bundle, directory, publish=args.publish, api_base=args.api_base.rstrip("/") or None,
                    resume=bool(args.resume) or recovering)
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
