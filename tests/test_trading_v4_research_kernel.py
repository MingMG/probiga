from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys

import pytest

from server.trading_v4.application import (
    ResearchBundleValidationError,
    build_forward_research_decision_input,
    run_forward_research_observation,
    validate_research_observation_bundle,
)
from server.trading_v4.domain import (
    AsOfDataset,
    AsOfRecord,
    CandidateStatus,
    DecisionBundleStatus,
    DecisionClock,
    QualityStatus,
)
from server.trading_v4.kernel import ResearchDecisionKernel, ResearchObservation
from tools.trading_v4.run_research import ResearchInputError, load_request
from tools.trading_v4 import release_integrity
from tools.trading_v4.release_integrity import validate_v4_release


TZ8 = timezone(timedelta(hours=8))
START = datetime(2026, 6, 1, 15, 0, tzinfo=TZ8)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _records(
    instrument: str,
    *,
    daily_return: Decimal,
    count: int = 30,
) -> tuple[AsOfRecord, ...]:
    records: list[AsOfRecord] = []
    previous = Decimal("10")
    for index in range(count):
        close = (previous * (Decimal("1") + daily_return)).quantize(
            Decimal("0.0001")
        )
        event_at = START + timedelta(days=index)
        records.append(
            AsOfRecord(
                record_id=f"{instrument}-bar-{index:03d}",
                source="explicit_forward_daily_bar",
                event_time=event_at,
                knowledge_time=event_at + timedelta(minutes=10),
                ingested_at=event_at + timedelta(minutes=5),
                payload={
                    "instrument": instrument,
                    "trade_date": event_at.date().isoformat(),
                    "open": previous,
                    "high": max(previous, close) * Decimal("1.002"),
                    "low": min(previous, close) * Decimal("0.998"),
                    "close": close,
                    "previous_close": previous,
                    "volume": Decimal("1000000"),
                    "amount": Decimal("1000000") * close,
                    "turnover_pct": Decimal("8"),
                    "upper_limit": previous * Decimal("1.10"),
                    "verified_capacity": True,
                    "is_suspended": False,
                },
            )
        )
        previous = close
    return tuple(records)


def _dataset(*, include_flat: bool = True) -> AsOfDataset:
    records = list(_records("600001.SH", daily_return=Decimal("0.01")))
    if include_flat:
        records.extend(_records("000001.SZ", daily_return=Decimal("0")))
    as_of = max(item.knowledge_time for item in records)
    return AsOfDataset(
        dataset_name="synthetic-forward-unit-fixture",
        as_of=as_of,
        records=tuple(records),
        quality_status=QualityStatus.PASS,
    )


def _run(*, include_flat: bool = True):
    dataset = _dataset(include_flat=include_flat)
    instruments = (
        ("600001.SH", "000001.SZ")
        if include_flat
        else ("600001.SH",)
    )
    return run_forward_research_observation(
        dataset,
        instruments=instruments,
        decision_time=dataset.as_of,
        valid_until=dataset.as_of + timedelta(hours=2),
        decision_clock=DecisionClock.AFTER_CLOSE,
        code_commit_sha="a" * 40,
        config_hash="b" * 64,
    )


def test_research_kernel_is_deterministic_and_never_emits_trade_claims():
    first = _run()
    second = _run()

    assert first == second
    assert first.result_hash == second.result_hash
    assert first.status is DecisionBundleStatus.WATCH_ONLY
    assert first.forecasts == ()
    assert first.actions == ()
    assert first.execution_intents == ()
    assert first.diagnostics["expected_return_estimated"] is False
    assert first.diagnostics["probability_estimated"] is False
    assert first.diagnostics["actionable_output_allowed"] is False
    assert first.diagnostics["paper_buy_outbox_open"] is False
    assert first.diagnostics["production_activation_allowed"] is False
    observations = first.diagnostics["observations"]
    assert [item["instrument"] for item in observations] == [
        "600001.SH",
        "000001.SZ",
    ]
    assert observations[0]["candidate_status"] == CandidateStatus.WATCH.value
    assert observations[1]["candidate_status"] == CandidateStatus.RESEARCH_ONLY.value
    assert all(
        "OOS_GATE_NOT_PASSED" in item["reason_codes"]
        and "FORWARD_ONLY_EVIDENCE" in item["reason_codes"]
        for item in observations
    )


def test_research_score_is_prefix_invariant_across_universe_membership():
    one = _run(include_flat=False)
    two = _run(include_flat=True)
    first_one = one.diagnostics["observations"][0]
    first_two = next(
        item
        for item in two.diagnostics["observations"]
        if item["instrument"] == "600001.SH"
    )
    assert first_one["heuristic_screening_score"] == first_two[
        "heuristic_screening_score"
    ]
    assert first_one["candidate_status"] == first_two["candidate_status"]


def test_missing_required_feature_value_fails_closed_without_a_score():
    dataset = _dataset(include_flat=False)
    decision_input = build_forward_research_decision_input(
        dataset,
        instruments=("600001.SH",),
        decision_time=dataset.as_of,
        valid_until=dataset.as_of + timedelta(hours=2),
        decision_clock=DecisionClock.AFTER_CLOSE,
        code_commit_sha="a" * 40,
        config_hash="b" * 64,
    )
    feature = decision_input.feature_vectors[0]
    values = dict(feature.values)
    values.pop("return_20d_pct")
    incomplete = replace(feature, values=values)
    decision_input = replace(decision_input, feature_vectors=(incomplete,))

    bundle = ResearchDecisionKernel().evaluate(decision_input)
    validate_research_observation_bundle(bundle)
    observation = bundle.diagnostics["observations"][0]
    assert bundle.status is DecisionBundleStatus.DATA_BLOCKED
    assert observation["candidate_status"] == CandidateStatus.DATA_BLOCKED.value
    assert observation["heuristic_screening_score"] is None
    assert "INVALID_OR_MISSING_RETURN_20D_PCT" in observation["reason_codes"]


def test_research_validator_rejects_status_or_disclosure_smuggling():
    bundle = _run(include_flat=False)
    diagnostics = dict(bundle.diagnostics)
    diagnostics["actionable_output_allowed"] = True
    tampered = replace(bundle, diagnostics=diagnostics)
    with pytest.raises(
        ResearchBundleValidationError,
        match="canonical evaluation",
    ):
        validate_research_observation_bundle(tampered)

    original = _run(include_flat=False)
    original_observation = dict(original.diagnostics["observations"][0])
    for field_name, forged_value in (
        ("heuristic_screening_score", Decimal("999999")),
        ("evidence_classification", "BACKTEST_READY"),
        ("feature_hash", "0" * 64),
    ):
        forged_observation = {**original_observation, field_name: forged_value}
        forged_diagnostics = dict(original.diagnostics)
        forged_diagnostics["observations"] = (forged_observation,)
        forged = replace(original, diagnostics=forged_diagnostics)
        with pytest.raises(ResearchBundleValidationError):
            validate_research_observation_bundle(forged)

    original = _run(include_flat=False)
    observation = original.diagnostics["observations"][0]
    internally_consistent_forgery = ResearchObservation(
        instrument=observation["instrument"],
        feature_hash=observation["feature_hash"],
        evidence_classification=observation["evidence_classification"],
        candidate_status=CandidateStatus.RESEARCH_ONLY,
        heuristic_screening_score=Decimal("0"),
        source_record_count=observation["source_record_count"],
        reason_codes=tuple(
            reason
            for reason in observation["reason_codes"]
            if reason != "HEURISTIC_WATCH_THRESHOLD_MET"
        )
        + ("HEURISTIC_WATCH_THRESHOLD_NOT_MET",),
    ).as_payload()
    forged_diagnostics = dict(original.diagnostics)
    forged_diagnostics.update(
        {
            "observations": (internally_consistent_forgery,),
            "observation_set_hash": "0" * 64,
            "watch_count": 0,
            "research_count": 1,
        }
    )
    forged = replace(
        original,
        status=DecisionBundleStatus.RESEARCH_ONLY,
        diagnostics=forged_diagnostics,
    )
    forged_diagnostics = dict(forged.diagnostics)
    from server.trading_v4.domain import deterministic_hash

    forged_diagnostics["observation_set_hash"] = deterministic_hash(
        forged.diagnostics["observations"]
    )
    forged = replace(forged, diagnostics=forged_diagnostics)
    with pytest.raises(ResearchBundleValidationError, match="canonical evaluation"):
        validate_research_observation_bundle(forged)


def test_forward_daily_bar_release_rejects_intraday_and_missing_event_time():
    dataset = _dataset(include_flat=False)
    common = {
        "instruments": ("600001.SH",),
        "decision_time": dataset.as_of,
        "valid_until": dataset.as_of + timedelta(hours=2),
        "code_commit_sha": "a" * 40,
        "config_hash": "b" * 64,
    }
    with pytest.raises(ValueError, match="requires AFTER_CLOSE"):
        build_forward_research_decision_input(
            dataset,
            decision_clock=DecisionClock.INTRADAY,
            **common,
        )
    records = list(dataset.records)
    records[-1] = replace(records[-1], event_time=None)
    missing_event_dataset = AsOfDataset(
        dataset_name=dataset.dataset_name,
        as_of=dataset.as_of,
        records=tuple(records),
        quality_status=dataset.quality_status,
    )
    with pytest.raises(ValueError, match="require event_time"):
        build_forward_research_decision_input(
            missing_event_dataset,
            decision_clock=DecisionClock.AFTER_CLOSE,
            **common,
        )


def test_after_close_requires_actual_local_close_time_and_one_offset() -> None:
    records = tuple(
        replace(
            record,
            event_time=record.event_time - timedelta(hours=6),
            knowledge_time=record.knowledge_time - timedelta(hours=6),
            ingested_at=record.ingested_at - timedelta(hours=6),
        )
        for record in _records("600001.SH", daily_return=Decimal("0.01"))
    )
    dataset = AsOfDataset(
        dataset_name="morning-data-mislabelled-after-close",
        as_of=max(item.knowledge_time for item in records),
        records=records,
        quality_status=QualityStatus.PASS,
    )
    with pytest.raises(ValueError, match="at or after 15:00"):
        build_forward_research_decision_input(
            dataset,
            instruments=("600001.SH",),
            decision_time=dataset.as_of,
            valid_until=dataset.as_of + timedelta(hours=1),
            decision_clock=DecisionClock.AFTER_CLOSE,
            code_commit_sha="a" * 40,
            config_hash="b" * 64,
        )

    regular = _dataset(include_flat=False)
    with pytest.raises(ValueError, match=r"must use \+08:00"):
        build_forward_research_decision_input(
            regular,
            instruments=("600001.SH",),
            decision_time=regular.as_of.astimezone(
                timezone.utc
            ),
            valid_until=(regular.as_of + timedelta(hours=1)).astimezone(
                timezone.utc
            ),
            decision_clock=DecisionClock.AFTER_CLOSE,
            code_commit_sha="a" * 40,
            config_hash="b" * 64,
        )

    misleading_plus_14 = regular.as_of.astimezone(timezone(timedelta(hours=14)))
    with pytest.raises(ValueError, match=r"must use \+08:00"):
        build_forward_research_decision_input(
            regular,
            instruments=("600001.SH",),
            decision_time=misleading_plus_14,
            valid_until=misleading_plus_14 + timedelta(hours=1),
            decision_clock=DecisionClock.AFTER_CLOSE,
            code_commit_sha="a" * 40,
            config_hash="b" * 64,
        )

def test_forward_release_rejects_caller_extended_freshness():
    dataset = _dataset(include_flat=False)
    common = {
        "instruments": ("600001.SH",),
        "decision_clock": DecisionClock.AFTER_CLOSE,
        "code_commit_sha": "a" * 40,
        "config_hash": "b" * 64,
    }
    with pytest.raises(ValueError, match="equal dataset.as_of"):
        build_forward_research_decision_input(
            dataset,
            decision_time=dataset.as_of + timedelta(days=1),
            valid_until=dataset.as_of + timedelta(days=1, hours=1),
            **common,
        )
    with pytest.raises(ValueError, match="cannot exceed 24 hours"):
        build_forward_research_decision_input(
            dataset,
            decision_time=dataset.as_of,
            valid_until=dataset.as_of + timedelta(hours=24, seconds=1),
            **common,
        )

    stale_dataset = AsOfDataset(
        dataset_name=dataset.dataset_name,
        as_of=dataset.as_of + timedelta(days=1),
        records=dataset.records,
        quality_status=dataset.quality_status,
    )
    with pytest.raises(ValueError, match="latest selected record"):
        build_forward_research_decision_input(
            stale_dataset,
            decision_time=stale_dataset.as_of,
            valid_until=stale_dataset.as_of + timedelta(hours=1),
            **common,
        )


def test_dataset_rejects_a_record_beyond_explicit_cutoff():
    records = _records("600001.SH", daily_return=Decimal("0.01"))
    with pytest.raises(ValueError, match="beyond its as_of"):
        AsOfDataset(
            dataset_name="future-leak-unit-fixture",
            as_of=records[-1].knowledge_time - timedelta(seconds=1),
            records=records,
            quality_status=QualityStatus.PASS,
        )


def _cli_document() -> dict:
    records = _records("600001.SH", daily_return=Decimal("0.01"))
    cutoff = max(item.knowledge_time for item in records)
    return {
        "schema_version": "probiga.trading-v4.forward-research-input.v1",
        "dataset_name": "synthetic-cli-unit-fixture",
        "dataset_quality_status": "PASS",
        "knowledge_cutoff": cutoff.isoformat(),
        "decision_time": cutoff.isoformat(),
        "valid_until": (cutoff + timedelta(hours=2)).isoformat(),
        "decision_clock": "AFTER_CLOSE",
        "instruments": ["600001.SH"],
        "records": [
            {
                "record_id": item.record_id,
                "source": item.source,
                "knowledge_time": item.knowledge_time.isoformat(),
                "ingested_at": item.ingested_at.isoformat(),
                "event_time": item.event_time.isoformat(),
                "quality_status": item.quality_status.value,
                "payload": {
                    key: str(value) if isinstance(value, Decimal) else value
                    for key, value in item.payload.items()
                },
            }
            for item in records
        ],
    }


def test_direct_v4_research_cli_runs_without_ambient_pythonpath(tmp_path: Path):
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(_cli_document(), ensure_ascii=False),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            "tools/trading_v4/run_research.py",
            "--input",
            str(request),
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["release_id"] == "trading_v4.1.0-research"
    assert result["lifecycle_status"] == "RESEARCH_ONLY"
    assert result["execution_boundary"] == {
        "actions_emitted": False,
        "execution_intents_emitted": False,
        "forecasts_emitted": False,
        "paper_orders_allowed": False,
        "real_orders_allowed": False,
    }
    assert result["bundle"]["execution_intents"] == []
    assert result["manifest_integrity_status"] == (
        "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"
    )


def test_v4_release_integrity_is_internal_and_blocked() -> None:
    result = validate_v4_release()

    assert result.status == "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"
    assert result.document["release_decision"] == "BLOCK"
    assert result.document["production_eligible"] is False


def test_v4_release_validator_rejects_reparse_owned_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    owner = tmp_path / "owned"
    owner.mkdir()
    target = owner / "payload.json"
    target.write_text("{}", encoding="utf-8")
    real_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda self: self == target or real_is_symlink(self),
    )

    with pytest.raises(release_integrity.V4ReleaseIntegrityError, match="reparse"):
        release_integrity._reject_reparse_points(target, owner)


@pytest.mark.parametrize(
    "text, message",
    [
        ('{"schema_version":"x","schema_version":"y"}', "duplicate JSON key"),
        ('{"schema_version":"probiga.trading-v4.forward-research-input.v1",'
         '"unexpected":1}', "unsupported fields"),
        ('{"schema_version":"probiga.trading-v4.forward-research-input.v1",'
         '"value":NaN}', "non-finite JSON value"),
    ],
)
def test_v4_cli_input_parser_fails_closed(
    tmp_path: Path, text: str, message: str
) -> None:
    request = tmp_path / "invalid.json"
    request.write_text(text, encoding="utf-8")

    with pytest.raises(ResearchInputError, match=message):
        load_request(request)


def test_documented_v4_mysql_acceptance_cli_resolves_local_package():
    completed = subprocess.run(
        [sys.executable, "tools/trading_v4_mysql_acceptance.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "trading_v4_mysql_acceptance.py" in completed.stdout
    assert "--url-env" in completed.stdout
