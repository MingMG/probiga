"""Seal enriched real evidence, then optionally run the permanent pair CLI.

Default: local preparation only. --run delegates real generation and --publish
to preview_market_decision; no report authoring or provider injection exists.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import preview_market_decision as permanent

EXPECTED_STAGES = ("deepseek_draft", "codex_draft", "deepseek_review", "codex_review", "codex_final", "deepseek_final_review",
                   "codex_final_2", "deepseek_final_review_2")
DEFAULT_EVIDENCE = ROOT / "artifacts/deepseek_auto_reports_20261101/evidence_enrichment"
DEFAULT_FACTS = ROOT / "artifacts/report_correction_20261101/digest.json"


def audit_inputs(facts_path: Path, evidence_dir: Path, *, next_trade_date: str) -> tuple[dict, dict]:
    """Pure local verification; does not seal files, connect DBs or call models."""
    from biz.review import market_decision as core
    from biz.review.research_contract import MAX_PACKET_BYTES
    stop = time.monotonic() + 30
    def gate():
        if time.monotonic() >= stop:
            raise RuntimeError("FAILED stage=enriched_preparation: total_deadline_exceeded")
    if tuple(core.STAGES) != EXPECTED_STAGES:
        raise RuntimeError("FAILED stage=workflow: expected bounded Codex revision and DeepSeek audit")
    paths = {"facts": facts_path.resolve(), "review_context": evidence_dir.resolve() / "context_review_enriched.json",
             "briefing_context": evidence_dir.resolve() / "context_briefing_enriched.json"}
    raw, data = {}, {}
    for key, path in paths.items():
        gate()
        data[key] = permanent._read_json(path)
        raw[key] = path.read_bytes()
    manifest_path = evidence_dir.resolve() / "enrichment_manifest.json"
    manifest = permanent._read_json(manifest_path)
    if manifest.get("state") != "COMPLETE":
        raise RuntimeError("FAILED stage=enriched_preparation: assembly is not complete")
    for kind, key in (("review", "review_context"), ("briefing", "briefing_context")):
        expected = manifest.get("outputs", {}).get(kind, {}).get("sha256")
        if expected != permanent._sha(raw[key]):
            raise RuntimeError("FAILED stage=enriched_preparation: context differs from assembly receipt")
        if not all(isinstance(data[key].get(name), dict) for name in ("technical_facts", "fundamental_facts")):
            raise RuntimeError("FAILED stage=enriched_preparation: enriched evidence groups are missing")
    source_receipts = []
    for source in manifest.get("source_snapshots", []):
        gate()
        path = Path(source["path"])
        path = path if path.is_absolute() else ROOT / path
        if not path.is_file() or path.stat().st_size > permanent.MAX_BUNDLE_BYTES:
            raise RuntimeError("FAILED stage=enriched_preparation: source snapshot unavailable or oversized")
        observed = permanent._sha(path.read_bytes())
        if observed != source.get("sha256"):
            raise RuntimeError("FAILED stage=enriched_preparation: source snapshot identity changed")
        source_receipts.append({"path": str(path), "sha256": observed})
    bundle = permanent.freeze_bundle(data["facts"], data["review_context"], data["briefing_context"],
                                     next_trade_date=next_trade_date, previous_research=None, source_raw=raw)
    frozen = permanent._source_ordered_contexts(bundle)
    packet = core.build_packet(frozen["facts"], report_kind="review", next_trade_date=next_trade_date,
        context=frozen["review_context"], previous_research=None, now=core._now(),
        source_digest_sha256=core.source_digest_identity(frozen["facts"]))
    packet_bytes = len(core._json(packet).encode("utf-8"))
    if packet_bytes > MAX_PACKET_BYTES:
        raise RuntimeError("FAILED stage=enriched_preparation: packet exceeds the fixed research budget")
    # This is a pure serialization probe, never an enqueued task or a fake
    # response. Final-stage request size depends on the actual provider drafts.
    from server.ai_bridge import research as transport
    messages, schema = core.build_stage_request("codex_draft", packet, core._load_rules()[0])
    probe = {"request_id": "00000000-0000-4000-8000-000000000000", "stage": "codex_draft",
             "messages": messages, "response_schema": schema, "rules_sha256": core.rules_identity(),
             "deadline_at": (datetime.now(timezone.utc) + timedelta(seconds=150)).isoformat()}
    probe["input_sha256"] = transport.research_input_sha256(probe)
    envelope = transport.encode_envelope(probe)
    bridge_bytes = len(transport.canonical_json(probe).encode("utf-8"))
    metrics = sorted({str(item.get("metric")) for item in packet["facts"].values()})
    if not {"technical_coverage", "fundamental_coverage"}.issubset(metrics):
        raise RuntimeError("FAILED stage=enriched_preparation: packet omitted enriched coverage")
    for group, metric in (("technical_facts", "technical_indicators"), ("fundamental_facts", "fundamental_metrics")):
        if data["review_context"][group].get("status") == "OBSERVED" and metric not in metrics:
            raise RuntimeError("FAILED stage=enriched_preparation: observed enriched evidence did not enter packet")
    cutoffs = {kind: {"cutoff_at": data[key].get("cutoff_at"), "market_data_asof_at": data[key].get("market_data_asof_at"),
                      "technical_acquired_at": data[key]["technical_facts"].get("acquired_at"),
                      "fundamental_acquired_at": data[key]["fundamental_facts"].get("acquired_at"),
                      "technical_status": data[key]["technical_facts"].get("status"),
                      "fundamental_status": data[key]["fundamental_facts"].get("status")}
               for kind, key in (("review", "review_context"), ("briefing", "briefing_context"))}
    summary = {"status": "PREPARED", "prepared_at": datetime.now(timezone.utc).isoformat(),
        "review_date": bundle["review_date"], "next_trade_date": next_trade_date, "bundle_sha256": bundle["bundle_sha256"],
        "rules_sha256": core.rules_identity(), "workflow": getattr(core, "WORKFLOW_VERSION", "UNKNOWN"),
        "stages": list(core.STAGES), "provider_stage_budget": len(core.STAGES), "accepted_provider_stage_counts": [6, 8],
        "data_cutoff_at": bundle["data_cutoff_at"], "source_receipts": bundle["source_receipts"],
        "enrichment_manifest_sha256": permanent._sha(manifest_path.read_bytes()), "original_source_snapshots": source_receipts,
        "observed_cutoffs": cutoffs, "review_packet": {"bytes": packet_bytes, "byte_limit": MAX_PACKET_BYTES,
            "remaining_bytes": MAX_PACKET_BYTES - packet_bytes, "fact_count": len(packet["facts"]), "metrics": metrics},
        "bridge_draft_request": {"decoded_bytes": bridge_bytes, "decoded_limit": transport.MAX_RAW_BYTES,
            "envelope_bytes": len(envelope.encode("ascii")), "envelope_limit": transport.MAX_ENVELOPE_BYTES,
            "queued": False},
        "previous_research": None, "report_database_write_invoked": False, "model_calls": 0, "wecom_delivery_invoked": False,
        "briefing_preflight": "requires the newly generated automatic review; no synthetic parent is constructed"}
    gate()
    return bundle, summary


def prepare_inputs(facts_path: Path, evidence_dir: Path, *, next_trade_date: str, output_dir: Path) -> tuple[Path, dict]:
    bundle, summary = audit_inputs(facts_path, evidence_dir, next_trade_date=next_trade_date)
    directory = output_dir.resolve() / ("prepared-" + str(uuid4()))
    directory.mkdir(parents=True, exist_ok=False)
    path = directory / "bundle.json"
    permanent._atomic(path, bundle)
    permanent._atomic(directory / "enrichment_manifest.json", (evidence_dir / "enrichment_manifest.json").read_bytes(), raw=True)
    permanent._atomic(directory / "preparation_receipt.json", summary)
    return path, summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--facts", type=Path, default=DEFAULT_FACTS)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--next-trade-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run", action="store_true", help="真实生成通过永久CLI执行；默认仅本地封存")
    parser.add_argument("--publish", action="store_true", help="随永久CLI原子发布并回读，无企业微信投递")
    parser.add_argument("--api-base", default="http://47.113.123.190")
    args = parser.parse_args(argv)
    if args.publish and not args.run:
        parser.error("--publish requires explicit --run")
    bundle_path, summary = prepare_inputs(args.facts, args.evidence_dir, next_trade_date=args.next_trade_date,
                                         output_dir=args.output_dir)
    command = ["--bundle", str(bundle_path), "--output-dir", str(args.output_dir.resolve() / "runs")]
    if args.publish:
        command += ["--publish", "--api-base", args.api_base.rstrip("/")]
    print(json.dumps({"status": "PREPARED", "bundle": str(bundle_path), "bundle_sha256": summary["bundle_sha256"],
                      "permanent_cli_arguments": command, "wecom_delivery_invoked": False}, ensure_ascii=False), flush=True)
    if args.run:
        from biz.review.market_decision import rules_identity
        if rules_identity() != summary["rules_sha256"]:
            raise RuntimeError("FAILED stage=workflow: rules changed after preparation")
        return permanent.main(command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
