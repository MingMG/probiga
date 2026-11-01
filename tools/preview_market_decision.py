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
    value = json.loads(path.read_bytes().decode("utf-8"))
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


def execute_run(engine, bundle: dict, directory: Path, *, publish=False, api_base=None, resume=False) -> dict:
    from biz.review.market_decision import DEADLINE_SECONDS, rules_identity, source_digest_identity, verify_automatic_research
    from biz.review.research_flow import attach_review, generate_briefing, publication_snapshot, persist_report_pair, verify_report_pair, load_digest
    validate_bundle(bundle)
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
    parser.add_argument("--publish", action="store_true", help="原子发布两份自动稿并完成生产API验收；不推送")
    parser.add_argument("--api-base", default="", help="发布时必填，例如 http://47.113.123.190")
    args = parser.parse_args(argv)
    if args.resume and args.bundle:
        parser.error("--resume already owns its frozen bundle; do not supply --bundle")
    if args.publish and not args.api_base:
        parser.error("--publish requires --api-base")
    from server.common.batch_db import create_batch_engine
    engine = create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 15, "write_timeout": 15})
    try:
        if args.resume:
            directory = args.resume.resolve()
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
        execute_run(engine, bundle, directory, publish=args.publish, api_base=args.api_base.rstrip("/") or None, resume=bool(args.resume))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
