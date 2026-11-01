from copy import deepcopy
import json
from pathlib import Path

import pytest

from tools import prepare_enriched_market_acceptance as entry
from tools import preview_market_decision as permanent


@pytest.fixture
def inputs(tmp_path):
    facts = {"review_date": "2026-10-30", "adjust_type": 0, "publish_status": "ready",
        "generated_at": "2026-11-01T18:00:00+08:00", "data_cutoff_at": "2026-11-01T15:32:38+08:00",
        "compact_review": "old editorial", "quality_json": {"target_date": "2026-10-30", "errors": [],
        "source_dates": {"target_bars": "2026-10-30"}}, "market_structure_json": {"sample_count": 2},
        "industry_rotation_json": {}, "factor_validation_json": {"market_decision": {"content": "old response"}}}
    context = {"cutoff_at": "2026-10-30T23:59:59+08:00", "market_data_asof_at": "2026-11-01T19:15:00+08:00",
        "technical_facts": {"status": "UNKNOWN", "stocks": [], "indices": [], "acquired_at": "2026-11-01T23:59:14+08:00"},
        "fundamental_facts": {"status": "UNKNOWN", "stocks": [], "acquired_at": "2026-11-02T00:08:15+08:00"}}
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    facts_path = tmp_path / "facts.json"
    facts_path.write_text(json.dumps(facts, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs = {}
    for kind in ("review", "briefing"):
        path = evidence / ("context_" + kind + "_enriched.json")
        path.write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs[kind] = {"sha256": permanent._sha(path.read_bytes())}
    permanent._atomic(evidence / "enrichment_manifest.json", {"state": "COMPLETE", "source_snapshots": [], "outputs": outputs})
    return facts_path, evidence, context


def test_preparation_preserves_raw_source_and_observed_cutoffs_without_publishing(inputs, tmp_path):
    facts, evidence, original = inputs
    path, receipt = entry.prepare_inputs(facts, evidence, next_trade_date="2026-11-02", output_dir=tmp_path / "prepared")
    bundle = permanent._read_json(path)
    permanent.validate_bundle(bundle)
    assert bundle["facts"]["compact_review"] == ""
    assert "market_decision" not in bundle["facts"]["factor_validation_json"]
    assert bundle["review_context"]["cutoff_at"] == original["cutoff_at"]
    assert bundle["review_context"]["technical_facts"]["acquired_at"] == original["technical_facts"]["acquired_at"]
    assert receipt["source_receipts"]["facts"]["raw_sha256"] == permanent._sha(facts.read_bytes())
    assert receipt["model_calls"] == 0 and receipt["report_database_write_invoked"] is False
    assert receipt["wecom_delivery_invoked"] is False
    assert receipt["provider_stage_budget"] == 8
    assert receipt["accepted_provider_stage_counts"] == [6, 7, 8]
    assert (path.parent / "enrichment_manifest.json").read_bytes() == (evidence / "enrichment_manifest.json").read_bytes()


def test_changed_enriched_context_is_rejected_before_bundle_creation(inputs):
    facts, evidence, _ = inputs
    path = evidence / "context_review_enriched.json"
    value = permanent._read_json(path)
    value["cutoff_at"] = "2026-11-01T23:59:59+08:00"
    permanent._atomic(path, value)
    with pytest.raises(RuntimeError, match="differs from assembly receipt"):
        entry.audit_inputs(facts, evidence, next_trade_date="2026-11-02")


def test_publish_requires_explicit_real_run(inputs, tmp_path):
    with pytest.raises(SystemExit) as failure:
        entry.main(["--next-trade-date", "2026-11-02", "--output-dir", str(tmp_path), "--publish"])
    assert failure.value.code == 2


def test_real_run_only_delegates_to_the_permanent_cli(inputs, tmp_path, monkeypatch):
    facts, evidence, _ = inputs
    captured = []
    monkeypatch.setattr(permanent, "main", lambda command: captured.append(command) or 0)
    assert entry.main(["--facts", str(facts), "--evidence-dir", str(evidence), "--next-trade-date", "2026-11-02",
        "--output-dir", str(tmp_path / "out"), "--run", "--publish", "--api-base", "http://example.invalid"]) == 0
    assert len(captured) == 1
    assert captured[0][-3:] == ["--publish", "--api-base", "http://example.invalid"]
    assert "--bundle" in captured[0] and "--resume" not in captured[0] and "--recover-from" not in captured[0]
