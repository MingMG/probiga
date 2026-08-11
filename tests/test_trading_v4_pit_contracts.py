from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from server.trading_v4.domain import (
    AsOfDataset,
    AsOfRecord,
    AvailabilityStatus,
    CertificationStatus,
    DataSourceCertification,
    FactorDefinition,
    FactorRole,
    FeatureVector,
    QualityStatus,
    ReplayEligibility,
    ResearchStatus,
    ScopeRef,
    ScopeType,
    SourceWatermark,
    deterministic_hash,
)
from server.trading_v4.pit import certify_prefix_invariance


AS_OF = datetime(2026, 8, 4, 6, 30, tzinfo=timezone.utc)


def _forward_source() -> DataSourceCertification:
    return DataSourceCertification(
        source_key="daily_bar",
        source_version="v4:source:daily-bar:1",
        adapter_version="v4:adapter:daily-bar:1",
        certification_version="v4:certification:daily-bar:1",
        replay_eligibility=ReplayEligibility.FORWARD_ONLY,
        certification_status=CertificationStatus.PASSED,
        availability_status=AvailabilityStatus.ACTIVE,
        research_status=ResearchStatus.FORWARD_ONLY,
        quality_status=QualityStatus.PASS,
        knowledge_time_field="knowledge_time",
        ingested_at_field="etl_sync_at",
        event_time_field="trade_time",
        revision_policy="UPSERT_NO_HISTORICAL_REVISION_CHAIN",
        allowed_fields=("stock_code", "trade_date", "close"),
        evidence_hashes=("a" * 64,),
        available_at=AS_OF - timedelta(days=1),
        assessed_at=AS_OF,
        reason_codes=("HISTORICAL_REVISION_CHAIN_UNAVAILABLE",),
    )


def _factor() -> FactorDefinition:
    return FactorDefinition(
        factor_key="chase_risk",
        factor_version="v4:factor:chase-risk:1",
        role=FactorRole.RISK,
        scope_type=ScopeType.INSTRUMENT,
        feature_set_version="v4:feature:price-risk:1",
        builder_version="v4:builder:price-risk:1",
        required_source_versions={
            "daily_bar": "v4:source:daily-bar:1"
        },
        required_fields={
            "daily_bar": (
                "stock_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
        },
        output_fields=(
            "conservative_surge_streak",
            "exact_limit_up_streak",
            "ordinary_buy_eligible",
        ),
        missing_policy="BLOCK",
        availability_status=AvailabilityStatus.ACTIVE,
        research_status=ResearchStatus.FORWARD_ONLY,
        quality_status=QualityStatus.PASS,
        available_at=AS_OF - timedelta(hours=1),
        formula_hash="b" * 64,
        max_age_seconds=86_400,
        specification={"four_board_status": "WATCH"},
    )


def test_forward_only_source_is_available_but_not_backtest_ready() -> None:
    source = _forward_source()

    assert source.is_available_as_of(AS_OF)
    assert not source.is_backtest_ready_as_of(AS_OF)
    assert source.certification_hash == _forward_source().certification_hash
    assert source.certification_id == _forward_source().certification_id


def test_pit_certified_source_requires_real_revision_evidence() -> None:
    with pytest.raises(ValueError, match="PIT_CERTIFIED"):
        replace(
            _forward_source(),
            replay_eligibility=ReplayEligibility.PIT_CERTIFIED,
            research_status=ResearchStatus.BACKTEST_READY,
            certified_from=AS_OF - timedelta(days=365),
            revision_policy="OVERWRITE_WITHOUT_HISTORY",
        )

    certified = replace(
        _forward_source(),
        replay_eligibility=ReplayEligibility.PIT_CERTIFIED,
        research_status=ResearchStatus.BACKTEST_READY,
        certified_from=AS_OF - timedelta(days=365),
        revision_policy="APPEND_ONLY_REVISION_CHAIN",
    )
    assert certified.is_backtest_ready_as_of(AS_OF)


def test_revision_policy_is_uppercase_and_unknown_policy_cannot_be_pit() -> None:
    forward = replace(_forward_source(), revision_policy="current_only")
    assert forward.revision_policy == "CURRENT_ONLY"

    with pytest.raises(ValueError, match="PIT_CERTIFIED"):
        replace(
            _forward_source(),
            replay_eligibility=ReplayEligibility.PIT_CERTIFIED,
            research_status=ResearchStatus.BACKTEST_READY,
            certified_from=AS_OF - timedelta(days=365),
            revision_policy="current_only",
        )

    with pytest.raises(ValueError, match="PIT_CERTIFIED"):
        replace(
            _forward_source(),
            replay_eligibility=ReplayEligibility.PIT_CERTIFIED,
            research_status=ResearchStatus.BACKTEST_READY,
            certified_from=AS_OF - timedelta(days=365),
            revision_policy="some_new_unknown_policy",
        )


def test_factor_definition_rejects_silent_neutral_missing_policy() -> None:
    with pytest.raises(ValueError, match="missing_policy"):
        replace(_factor(), missing_policy="FILL_NEUTRAL_ZERO")

    first = _factor()
    second = replace(
        _factor(),
        required_fields={
            "daily_bar": tuple(reversed(_factor().required_fields["daily_bar"]))
        },
    )
    assert first.definition_hash == second.definition_hash
    assert first.actionable is False


def test_feature_vector_requires_explicit_freshness_and_missing_reasons() -> None:
    common = {
        "scope": ScopeRef(ScopeType.INSTRUMENT, "603221.SH"),
        "feature_set_version": "v4:feature:price-risk:1",
        "feature_builder_version": "v4:builder:price-risk:1",
        "capability_name": "daily_bar",
        "source_manifest_hash": "c" * 64,
        "knowledge_time": AS_OF,
        "values": {"ordinary_buy_eligible": False},
        "source_record_ids": ("bar-1",),
        "source_record_hashes": {"bar-1": deterministic_hash("bar-1")},
    }
    with pytest.raises(ValueError, match="valid_until"):
        FeatureVector(valid_until=AS_OF - timedelta(seconds=1), **common)
    with pytest.raises(ValueError, match="missing fields"):
        FeatureVector(
            valid_until=AS_OF + timedelta(minutes=5),
            missing_fields=("exact_limit_up_streak",),
            **common,
        )
    with pytest.raises(ValueError, match="requires reason_codes"):
        FeatureVector(
            valid_until=AS_OF + timedelta(minutes=5),
            quality_status=QualityStatus.WARN,
            missing_fields=("exact_limit_up_streak",),
            **common,
        )


def test_source_watermark_cannot_claim_pass_for_partial_coverage() -> None:
    with pytest.raises(ValueError, match="complete coverage"):
        SourceWatermark(
            source="daily_bar",
            knowledge_time=AS_OF,
            record_count=9,
            quality_status=QualityStatus.PASS,
            snapshot_id="snapshot-1",
            coverage="0.9",  # type: ignore[arg-type]
        )


def _record(record_id: str, knowledge_time: datetime, close: str) -> AsOfRecord:
    return AsOfRecord(
        record_id=record_id,
        source="daily_bar",
        knowledge_time=knowledge_time,
        ingested_at=knowledge_time,
        event_time=knowledge_time - timedelta(minutes=1),
        payload={"instrument": "603221.SH", "close": close},
    )


def _prefix_builder(dataset: AsOfDataset) -> tuple[FeatureVector, ...]:
    if not dataset.records:
        return ()
    latest = dataset.records[-1]
    return (
        FeatureVector(
            scope=ScopeRef(ScopeType.INSTRUMENT, "603221.SH"),
            feature_set_version="v4:feature:prefix-probe:1",
            feature_builder_version="v4:builder:prefix-probe:1",
            capability_name="daily_bar",
            source_manifest_hash=dataset.manifest_hash,
            knowledge_time=max(record.knowledge_time for record in dataset.records),
            valid_until=dataset.as_of + timedelta(minutes=5),
            values={"latest_close": latest.payload["close"]},
            source_record_ids=tuple(record.record_id for record in dataset.records),
            source_record_hashes={
                record.record_id: record.record_hash for record in dataset.records
            },
        ),
    )


def test_prefix_invariance_ignores_future_append_but_rejects_backdated_change() -> None:
    known = _record("bar-known", AS_OF - timedelta(hours=1), "22.54")
    future = _record("bar-future", AS_OF + timedelta(hours=1), "24.79")
    baseline = AsOfDataset(
        dataset_name="daily-bars",
        as_of=AS_OF,
        records=(known,),
        quality_status=QualityStatus.PASS,
    )
    extended = AsOfDataset(
        dataset_name="daily-bars",
        as_of=AS_OF + timedelta(hours=2),
        records=(known, future),
        quality_status=QualityStatus.PASS,
    )
    passed = certify_prefix_invariance(
        baseline,
        extended,
        cutoff=AS_OF,
        builder=_prefix_builder,
    )
    assert passed.passed

    rewritten = _record("bar-known", AS_OF - timedelta(hours=1), "99.99")
    changed = AsOfDataset(
        dataset_name="daily-bars",
        as_of=AS_OF + timedelta(hours=2),
        records=(rewritten, future),
        quality_status=QualityStatus.PASS,
    )
    failed = certify_prefix_invariance(
        baseline,
        changed,
        cutoff=AS_OF,
        builder=_prefix_builder,
    )
    assert not failed.passed
    assert failed.reason_codes == ("PREFIX_CHANGED_AFTER_FUTURE_APPEND",)
