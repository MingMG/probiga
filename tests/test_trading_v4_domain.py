from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.trading_v4.domain import (
    AccountSnapshot,
    ActionType,
    AsOfDataset,
    AsOfRecord,
    AvailabilityStatus,
    CalibrationArtifactRef,
    CandidateStatus,
    CapabilityStatus,
    CommitStatus,
    CommittedExecutionIntent,
    DataManifest,
    DecisionAction,
    DecisionBundle,
    DecisionBundleStatus,
    DecisionClock,
    DecisionCommitReceipt,
    DecisionContext,
    DecisionInput,
    DatasetResult,
    ExecutionIntent,
    ExecutionSide,
    FeatureVector,
    ForecastResult,
    InstrumentRuleSnapshot,
    LimitPolicy,
    ModelArtifactRef,
    PositionSnapshot,
    ProbabilityKind,
    QualityStatus,
    ResearchStatus,
    ScopeRef,
    ScopeType,
    SourceWatermark,
    deterministic_hash,
    derive_decision_id,
)
from server.trading_v4.ports import ModelRegistryPort
from server.trading_v4.kernel import BlockedDecisionKernel, DecisionKernel


CHINA = timezone(timedelta(hours=8))
DECISION_TIME = datetime(2026, 8, 3, 14, 30, tzinfo=CHINA)
CUTOFF = DECISION_TIME - timedelta(seconds=1)


def _data_manifest(*, reverse: bool = False) -> DataManifest:
    record_hashes = {
        record_id: deterministic_hash(record_id)
        for record_id in (
            "bar-1",
            "bar-2",
            "market-1",
            "future-1",
            "other-1",
        )
    }
    if reverse:
        record_hashes = dict(reversed(tuple(record_hashes.items())))
    return DataManifest(record_hashes=record_hashes)


def _context(*, reverse_mappings: bool = False) -> DecisionContext:
    watermarks = {
        "daily_bar": SourceWatermark(
            source="daily_bar",
            knowledge_time=CUTOFF - timedelta(minutes=1),
            record_count=2,
            quality_status=QualityStatus.PASS,
            snapshot_id="daily-1",
            valid_until=DECISION_TIME + timedelta(minutes=5),
            coverage=Decimal("1"),
            batch_id="daily-batch-1",
            schema_version="daily-schema-v1",
            content_hash="1" * 64,
        ),
        "account": SourceWatermark(
            source="account",
            knowledge_time=CUTOFF,
            record_count=1,
            quality_status=QualityStatus.PASS,
            snapshot_id="account-1",
            valid_until=DECISION_TIME + timedelta(minutes=5),
            coverage=Decimal("1"),
            batch_id="account-batch-1",
            schema_version="account-schema-v1",
            content_hash="2" * 64,
        ),
    }
    capabilities = {
        "daily_bar": CapabilityStatus(
            name="daily_bar",
            availability_status=AvailabilityStatus.ACTIVE,
            research_status=ResearchStatus.BACKTEST_READY,
            quality_status=QualityStatus.PASS,
        ),
        "order_book": CapabilityStatus(
            name="order_book",
            availability_status=AvailabilityStatus.BLOCKED,
            research_status=ResearchStatus.FORWARD_ONLY,
            quality_status=QualityStatus.FAIL,
            reason_codes=("NO_HISTORY",),
        ),
    }
    if reverse_mappings:
        watermarks = dict(reversed(tuple(watermarks.items())))
        capabilities = dict(reversed(tuple(capabilities.items())))
    return DecisionContext(
        decision_time=DECISION_TIME,
        decision_clock=DecisionClock.INTRADAY,
        knowledge_cutoff=CUTOFF,
        trade_date=date(2026, 8, 3),
        universe_version="universe-v1",
        data_manifest=_data_manifest(reverse=reverse_mappings),
        portfolio_policy_version="portfolio-v1",
        execution_contract_version="execution-v1",
        fee_schedule_version="fees-v1",
        account_snapshot_id="account-snapshot-1",
        code_commit_sha="b" * 40,
        config_hash="c" * 64,
        random_seed=42,
        source_watermarks=watermarks,
        factor_spec_versions={"trend": "2", "risk": "1"},
        forecast_contract_ids=("holding-v1", "next-session-v1"),
        model_versions={"stock": "v4:model-7", "market": "v4:model-2"},
        model_artifact_hashes={"stock": "d" * 64, "market": "e" * 64},
        model_training_cutoffs={
            "stock": CUTOFF - timedelta(days=30),
            "market": CUTOFF - timedelta(days=30),
        },
        model_available_at={
            "stock": CUTOFF - timedelta(days=1),
            "market": CUTOFF - timedelta(days=1),
        },
        calibration_versions={"stock": "v4:calibration-3"},
        calibration_artifact_hashes={"stock": "f" * 64},
        calibration_training_cutoffs={
            "stock": CUTOFF - timedelta(days=30)
        },
        calibration_available_at={
            "stock": CUTOFF - timedelta(days=1)
        },
        capability_statuses=capabilities,
    )


def test_pass_watermark_requires_nonempty_fresh_evidence() -> None:
    with pytest.raises(ValueError, match="non-empty evidence"):
        SourceWatermark(
            source="empty_daily_bar",
            knowledge_time=CUTOFF,
            record_count=0,
            quality_status=QualityStatus.PASS,
            snapshot_id="empty-snapshot",
            valid_until=DECISION_TIME + timedelta(minutes=1),
            coverage=Decimal("1"),
            batch_id="empty-batch",
            schema_version="daily-schema-v1",
            content_hash="f" * 64,
        )


def test_decision_context_rejects_expired_required_watermark() -> None:
    base = _context()
    expired = replace(
        base.source_watermarks["daily_bar"],
        valid_until=DECISION_TIME - timedelta(microseconds=1),
    )
    with pytest.raises(ValueError, match="expired before decision_time"):
        replace(
            base,
            source_watermarks={
                **dict(base.source_watermarks),
                "daily_bar": expired,
            },
        )


def _account() -> AccountSnapshot:
    return AccountSnapshot(
        account_snapshot_id="account-snapshot-1",
        account_id="paper-v4-shadow-e1",
        as_of=CUTOFF,
        available_cash=Decimal("100000.00"),
        equity=Decimal("125000.00"),
        positions=(
            PositionSnapshot(
                instrument="600001.SH",
                total_quantity=1000,
                sellable_quantity=0,
                average_cost=Decimal("24.00"),
                last_price=Decimal("25.00"),
                origin_strategy="V4",
            ),
        ),
    )


def _features() -> tuple[FeatureVector, FeatureVector]:
    return (
        FeatureVector(
            scope=ScopeRef(ScopeType.INSTRUMENT, "600001.SH"),
            feature_set_version="stock-v1",
            feature_builder_version="builder-v1",
            capability_name="daily_bar",
            source_manifest_hash=_data_manifest().manifest_hash,
            knowledge_time=CUTOFF,
            valid_until=DECISION_TIME + timedelta(minutes=5),
            values={"return_5d_pct": Decimal("3.20"), "tradable": True},
            source_record_ids=("bar-2", "bar-1"),
            source_record_hashes={
                "bar-2": deterministic_hash("bar-2"),
                "bar-1": deterministic_hash("bar-1"),
            },
        ),
        FeatureVector(
            scope=ScopeRef(ScopeType.MARKET, "CN_A"),
            feature_set_version="market-v1",
            feature_builder_version="builder-v1",
            capability_name="daily_bar",
            source_manifest_hash=_data_manifest().manifest_hash,
            knowledge_time=CUTOFF - timedelta(seconds=2),
            valid_until=DECISION_TIME + timedelta(minutes=5),
            values={"breadth_pct": Decimal("58.0")},
            source_record_ids=("market-1",),
            source_record_hashes={
                "market-1": deterministic_hash("market-1")
            },
        ),
    )


def _rules() -> tuple[InstrumentRuleSnapshot, ...]:
    return (
        InstrumentRuleSnapshot(
            instrument="600001.SH",
            rule_version="sse-main-v1",
            effective_at=DECISION_TIME.replace(hour=9, minute=15),
            knowledge_time=CUTOFF,
            valid_until=DECISION_TIME + timedelta(days=1),
            can_buy=True,
            can_sell=True,
            first_buy_minimum=100,
            buy_lot_size=100,
            sell_lot_size=1,
            settlement_days=1,
            tick_size=Decimal("0.01"),
            allow_odd_lot_liquidation=True,
            upper_limit=Decimal("30.00"),
            lower_limit=Decimal("20.00"),
        ),
    )


def _decision_input() -> DecisionInput:
    features = _features()
    return DecisionInput(
        context=_context(),
        account=_account(),
        scopes=tuple(item.scope for item in features),
        feature_vectors=features,
        instrument_rules=_rules(),
    )


def test_instrument_rule_snapshot_rejects_ambiguous_price_facts_and_flags():
    rule = _rules()[0]
    with pytest.raises(TypeError, match="can_buy must be a bool"):
        replace(rule, can_buy=1)
    with pytest.raises(ValueError, match="supplied together"):
        replace(rule, upper_limit=None)
    with pytest.raises(ValueError, match="must not be below"):
        replace(
            rule,
            lower_limit=Decimal("31.00"),
            upper_limit=Decimal("30.00"),
        )
    with pytest.raises(ValueError, match="must be positive"):
        replace(rule, lower_limit=Decimal("0"), upper_limit=Decimal("30"))
    with pytest.raises(ValueError, match="valid_until"):
        replace(rule, valid_until=rule.effective_at - timedelta(seconds=1))


def test_canonical_hash_normalizes_mapping_decimal_and_timezone():
    utc_instant = DECISION_TIME.astimezone(timezone.utc)
    first = {"b": Decimal("1.00"), "a": DECISION_TIME}
    second = {"a": utc_instant, "b": Decimal("1")}
    assert deterministic_hash(first) == deterministic_hash(second)


def test_decision_context_is_deterministic_and_deeply_immutable():
    first = _context()
    second = _context(reverse_mappings=True)

    assert first.context_id == second.context_id
    assert first.context_hash == second.context_hash
    with pytest.raises(TypeError):
        first.model_versions["stock"] = "changed"
    with pytest.raises(FrozenInstanceError):
        first.random_seed = 99


def test_data_manifest_is_deterministic_and_enforces_exact_membership():
    first = _data_manifest()
    second = _data_manifest(reverse=True)
    assert first.manifest_hash == second.manifest_hash
    assert first.contains_exact_subset(
        {"bar-1": deterministic_hash("bar-1")}
    )
    assert not first.contains_exact_subset({"bar-1": "9" * 64})
    with pytest.raises(TypeError):
        first.record_hashes["new"] = "1" * 64


def test_normalized_mapping_keys_cannot_collide_silently():
    with pytest.raises(ValueError, match="duplicate keys after normalization"):
        DataManifest(
            record_hashes={
                "bar-1": deterministic_hash("real"),
                " bar-1 ": deterministic_hash("forged"),
            }
        )

    feature = _features()[0]
    with pytest.raises(ValueError, match="duplicate keys after normalization"):
        replace(
            feature,
            source_record_ids=("bar-1",),
            source_record_hashes={
                "bar-1": deterministic_hash("bar-1"),
                " bar-1 ": deterministic_hash("forged"),
            },
        )

    with pytest.raises(ValueError, match="duplicate keys after normalization"):
        replace(
            _context(),
            factor_spec_versions={"trend": "2", " trend ": "3"},
        )


def test_legacy_field_guard_normalizes_surrounding_whitespace():
    feature = _features()[0]
    with pytest.raises(ValueError, match="forbidden legacy field"):
        replace(feature, values={" V3_FORECAST ": Decimal("0.9")})


def test_context_rejects_future_model_and_non_v4_artifact_namespace():
    context = _context()
    with pytest.raises(ValueError, match="availability exceeds"):
        replace(
            context,
            model_available_at={
                "stock": DECISION_TIME,
                "market": CUTOFF - timedelta(days=1),
            },
        )
    with pytest.raises(ValueError, match="training cutoff exceeds"):
        replace(
            context,
            model_training_cutoffs={
                "stock": CUTOFF,
                "market": CUTOFF - timedelta(days=30),
            },
        )
    with pytest.raises(ValueError, match="namespace"):
        replace(
            context,
            model_versions={"stock": "model-7", "market": "v4:model-2"},
        )
    with pytest.raises(ValueError, match="namespace"):
        replace(_forecast(), calibration_version="calibration-3")
    with pytest.raises(ValueError, match="calibration availability exceeds"):
        replace(
            context,
            calibration_available_at={"stock": DECISION_TIME},
        )


def test_artifact_refs_require_v4_versions_and_active_point_in_time_status():
    model = ModelArtifactRef(
        model_id="stock",
        model_version="v4:model-7",
        artifact_hash="a" * 64,
        training_cutoff=CUTOFF - timedelta(days=30),
        feature_spec_version="stock-v1",
        forecast_contract_id="next-session-v1",
        calibration_artifact_hash="b" * 64,
        promoted_at=CUTOFF - timedelta(days=1),
        status="ACTIVE",
    )
    calibration = CalibrationArtifactRef(
        calibration_id="stock",
        calibration_version="v4:calibration-3",
        artifact_hash="c" * 64,
        training_cutoff=CUTOFF - timedelta(days=20),
        model_id="stock",
        model_version="v4:model-7",
        forecast_contract_id="next-session-v1",
        promoted_at=CUTOFF - timedelta(hours=12),
        status="ACTIVE",
    )

    assert model.is_available_as_of(CUTOFF)
    assert calibration.is_available_as_of(CUTOFF)
    assert not model.is_available_as_of(model.promoted_at - timedelta(seconds=1))
    assert not replace(model, status="REVOKED").is_available_as_of(CUTOFF)
    assert not replace(calibration, status="BLOCKED").is_available_as_of(CUTOFF)
    with pytest.raises(ValueError, match="namespace"):
        replace(calibration, calibration_version="calibration-3")
    with pytest.raises(ValueError, match="namespace"):
        replace(calibration, model_version="model-7")
    assert hasattr(ModelRegistryPort, "resolve_calibration")


def test_forward_only_capability_is_not_actionable():
    capability = CapabilityStatus(
        name="order_book",
        availability_status=AvailabilityStatus.ACTIVE,
        research_status=ResearchStatus.FORWARD_ONLY,
        quality_status=QualityStatus.PASS,
    )

    assert capability.actionable is False


def test_asof_dataset_rejects_future_knowledge():
    record = AsOfRecord(
        record_id="event-1",
        source="exchange",
        knowledge_time=DECISION_TIME,
        ingested_at=DECISION_TIME,
        payload={"title": "risk notice"},
    )
    with pytest.raises(ValueError, match="beyond its as_of"):
        AsOfDataset(
            dataset_name="events",
            as_of=CUTOFF,
            records=(record,),
            quality_status=QualityStatus.PASS,
        )


def test_asof_record_rejects_knowledge_before_ingestion():
    with pytest.raises(ValueError, match="acquisition timestamp"):
        AsOfRecord(
            record_id="late-event",
            source="exchange",
            knowledge_time=CUTOFF,
            ingested_at=DECISION_TIME,
            payload={"title": "late"},
        )


def test_dataset_result_exposes_coverage_and_rejects_false_pass():
    record = AsOfRecord(
        record_id="bar-600001",
        source="exchange",
        knowledge_time=CUTOFF,
        ingested_at=CUTOFF,
        payload={"instrument": "600001.SH", "close": "25.00"},
    )
    dataset = AsOfDataset(
        dataset_name="daily-bars",
        as_of=CUTOFF,
        records=(record,),
        quality_status=QualityStatus.PASS,
    )
    partial = DatasetResult(
        dataset=dataset,
        requested_cutoff=CUTOFF,
        requested_entities=("600001.SH", "600002.SH"),
        returned_entities=("600001.SH",),
        requested_fields=("close",),
        freshness_status=QualityStatus.WARN,
        reason_codes=("PARTIAL_COVERAGE",),
    )
    assert partial.coverage == Decimal("0.5")
    assert partial.missing_entities == ("600002.SH",)

    with pytest.raises(ValueError, match="complete"):
        DatasetResult(
            dataset=dataset,
            requested_cutoff=CUTOFF,
            requested_entities=("600001.SH", "600002.SH"),
            returned_entities=("600001.SH",),
            requested_fields=("close",),
            freshness_status=QualityStatus.PASS,
        )


def test_owned_aggregate_boundaries_reject_duck_types_and_subclasses():
    context = _context()
    watermarks = dict(context.source_watermarks)
    watermarks["daily_bar"] = object()
    with pytest.raises(TypeError, match="exactly SourceWatermark"):
        replace(context, source_watermarks=watermarks)

    capabilities = dict(context.capability_statuses)
    capabilities["daily_bar"] = object()
    with pytest.raises(TypeError, match="exactly CapabilityStatus"):
        replace(context, capability_statuses=capabilities)

    decision_input = _decision_input()
    with pytest.raises(TypeError, match="exactly ScopeRef"):
        replace(decision_input, scopes=(object(),))
    with pytest.raises(TypeError, match="exactly FeatureVector"):
        replace(decision_input, feature_vectors=(object(),))
    with pytest.raises(TypeError, match="exactly InstrumentRuleSnapshot"):
        replace(decision_input, instrument_rules=(object(),))
    with pytest.raises(TypeError, match="exactly PositionSnapshot"):
        replace(decision_input.account, positions=(object(),))

    decision_id = derive_decision_id(decision_input, "kernel-v1")
    with pytest.raises(TypeError, match="exactly ForecastResult"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.RESEARCH_ONLY,
            forecasts=(object(),),
        )
    with pytest.raises(TypeError, match="exactly DecisionAction"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.RESEARCH_ONLY,
            actions=(object(),),
        )
    with pytest.raises(TypeError, match="exactly ExecutionIntent"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.RESEARCH_ONLY,
            execution_intents=(object(),),
        )


def test_feature_vector_rejects_mutable_nested_dataclass():
    @dataclass
    class MutableValue:
        score: int

    with pytest.raises(TypeError, match="nested dataclasses"):
        FeatureVector(
            scope=ScopeRef(ScopeType.MARKET, "CN_A"),
            feature_set_version="market-v1",
            feature_builder_version="builder-v1",
            capability_name="daily_bar",
            source_manifest_hash=_data_manifest().manifest_hash,
            knowledge_time=CUTOFF,
            valid_until=DECISION_TIME + timedelta(minutes=5),
            values={"unsafe": MutableValue(1)},
        )


def test_feature_vector_rejects_legacy_opinion_fields_recursively():
    with pytest.raises(ValueError, match="forbidden legacy field"):
        FeatureVector(
            scope=ScopeRef(ScopeType.INSTRUMENT, "600001.SH"),
            feature_set_version="bad-v1",
            feature_builder_version="builder-v1",
            capability_name="daily_bar",
            source_manifest_hash=_data_manifest().manifest_hash,
            knowledge_time=CUTOFF,
            valid_until=DECISION_TIME + timedelta(minutes=5),
            values={"nested": {"v3_forecast": 0.9}},
        )


def test_decision_input_hash_is_order_independent_and_pit_guarded():
    features = _features()
    scopes = (features[0].scope, features[1].scope)
    first = DecisionInput(
        context=_context(),
        account=_account(),
        scopes=scopes,
        feature_vectors=features,
        instrument_rules=_rules(),
    )
    second = DecisionInput(
        context=_context(reverse_mappings=True),
        account=_account(),
        scopes=tuple(reversed(scopes)),
        feature_vectors=tuple(reversed(features)),
        instrument_rules=tuple(reversed(_rules())),
    )
    assert first.input_hash == second.input_hash

    future_feature = FeatureVector(
        scope=scopes[0],
        feature_set_version="future-v1",
        feature_builder_version="builder-v1",
        capability_name="daily_bar",
        source_manifest_hash=_data_manifest().manifest_hash,
        knowledge_time=DECISION_TIME,
        valid_until=DECISION_TIME + timedelta(minutes=5),
        values={"return_1d_pct": 1.0},
        source_record_ids=("future-1",),
        source_record_hashes={
            "future-1": deterministic_hash("future-1")
        },
    )
    with pytest.raises(ValueError, match="knowledge_cutoff"):
        DecisionInput(
            context=_context(),
            account=_account(),
            scopes=scopes,
            feature_vectors=(future_feature,),
            instrument_rules=_rules(),
        )

    undeclared_feature = FeatureVector(
        scope=ScopeRef(ScopeType.INSTRUMENT, "000002.SZ"),
        feature_set_version="stock-v1",
        feature_builder_version="builder-v1",
        capability_name="daily_bar",
        source_manifest_hash=_data_manifest().manifest_hash,
        knowledge_time=CUTOFF,
        valid_until=DECISION_TIME + timedelta(minutes=5),
        values={"return_5d_pct": Decimal("2.0")},
        source_record_ids=("other-1",),
        source_record_hashes={
            "other-1": deterministic_hash("other-1")
        },
    )
    with pytest.raises(ValueError, match="absent from decision scopes"):
        DecisionInput(
            context=_context(),
            account=_account(),
            scopes=scopes,
            feature_vectors=(undeclared_feature,),
            instrument_rules=_rules(),
        )


def test_decision_input_rejects_unbound_feature_manifest():
    feature = FeatureVector(
        scope=ScopeRef(ScopeType.INSTRUMENT, "600001.SH"),
        feature_set_version="stock-v1",
        feature_builder_version="builder-v1",
        capability_name="daily_bar",
        source_manifest_hash="d" * 64,
        knowledge_time=CUTOFF,
        valid_until=DECISION_TIME + timedelta(minutes=5),
        values={"return_5d_pct": Decimal("2.0")},
        source_record_ids=("bar-1",),
        source_record_hashes={"bar-1": deterministic_hash("bar-1")},
    )
    with pytest.raises(ValueError, match="source manifest"):
        DecisionInput(
            context=_context(),
            account=_account(),
            scopes=(feature.scope,),
            feature_vectors=(feature,),
            instrument_rules=_rules(),
        )


def test_decision_input_rejects_forged_record_hash_membership():
    decision_input = _decision_input()
    forged = replace(
        decision_input.feature_vectors[0],
        source_record_hashes={
            record_id: "9" * 64
            for record_id in decision_input.feature_vectors[0].source_record_ids
        },
    )
    with pytest.raises(ValueError, match="absent from data manifest"):
        DecisionInput(
            context=decision_input.context,
            account=decision_input.account,
            scopes=decision_input.scopes,
            feature_vectors=(forged, decision_input.feature_vectors[1]),
            instrument_rules=decision_input.instrument_rules,
        )


def test_exit_cannot_exceed_sellable_quantity():
    with pytest.raises(ValueError, match="sellable_quantity"):
        DecisionAction(
            decision_id="decision-exit",
            instrument="600001.SH",
            desired_action=ActionType.EXIT,
            executable_action=ActionType.EXIT,
            current_quantity=1000,
            sellable_quantity=0,
            target_quantity=0,
            earliest_execution_time=DECISION_TIME,
            valid_until=DECISION_TIME + timedelta(minutes=1),
            candidate_status=CandidateStatus.PAPER_ACTIONABLE,
        )


def test_desired_and_executable_actions_cannot_reverse_direction():
    with pytest.raises(ValueError, match="not allowed for desired_action"):
        DecisionAction(
            decision_id="decision-direction",
            instrument="600001.SH",
            desired_action=ActionType.EXIT,
            executable_action=ActionType.ADD,
            current_quantity=1000,
            sellable_quantity=1000,
            target_quantity=1100,
            earliest_execution_time=DECISION_TIME,
            valid_until=DECISION_TIME + timedelta(minutes=1),
            candidate_status=CandidateStatus.PAPER_ACTIONABLE,
        )


def _forecast() -> ForecastResult:
    return ForecastResult(
        scope=ScopeRef(ScopeType.INSTRUMENT, "600001.SH"),
        forecast_contract_id="next-session-v1",
        model_id="stock",
        model_version="v4:model-7",
        calibration_id="stock",
        calibration_version="v4:calibration-3",
        signal_at=CUTOFF,
        valid_until=DECISION_TIME + timedelta(days=1),
        expected_return_net_pct=Decimal("0.018"),
        cvar95_loss_pct=Decimal("0.025"),
        probability_positive=Decimal("0.62"),
        confidence=Decimal("0.71"),
        probability_kind=ProbabilityKind.MODEL_PREDICTED,
        status=CandidateStatus.PAPER_ACTIONABLE,
    )


def _action(
    decision_id: str,
    *,
    forecast_ids: tuple[str, ...] = (),
) -> DecisionAction:
    return DecisionAction(
        decision_id=decision_id,
        instrument="600001.SH",
        desired_action=ActionType.ADD,
        executable_action=ActionType.ADD,
        current_quantity=1000,
        sellable_quantity=0,
        target_quantity=1100,
        earliest_execution_time=DECISION_TIME + timedelta(seconds=1),
        valid_until=DECISION_TIME + timedelta(minutes=5),
        candidate_status=CandidateStatus.PAPER_ACTIONABLE,
        reason_codes=("RISK_GATE_PASS", "ALPHA_GATE_PASS"),
        forecast_ids=forecast_ids,
    )


def _intent(action: DecisionAction) -> ExecutionIntent:
    return ExecutionIntent(
        strategy_id="trading-v4-clean",
        decision_id=action.decision_id,
        action_id=action.action_id,
        account_id="paper-v4-shadow-e1",
        instrument=action.instrument,
        side=ExecutionSide.BUY,
        desired_quantity=100,
        target_quantity=1100,
        limit_policy=LimitPolicy.MARKETABLE_LIMIT,
        earliest_at=action.earliest_execution_time,
        valid_until=action.valid_until,
        execution_contract_version="execution-v1",
        limit_price=Decimal("25.10"),
    )


def test_execution_intent_idempotency_key_is_derived_and_tamper_evident():
    action = _action("decision-1")
    first = _intent(action)
    second = _intent(action)
    assert first.idempotency_key == second.idempotency_key

    with pytest.raises(ValueError, match="idempotency_key"):
        ExecutionIntent(
            strategy_id=first.strategy_id,
            decision_id=first.decision_id,
            action_id=first.action_id,
            account_id=first.account_id,
            instrument=first.instrument,
            side=first.side,
            desired_quantity=first.desired_quantity,
            target_quantity=first.target_quantity,
            limit_policy=first.limit_policy,
            earliest_at=first.earliest_at,
            valid_until=first.valid_until,
            execution_contract_version=first.execution_contract_version,
            idempotency_key="forged",
        )


def test_execution_submission_requires_commit_proof_and_derived_outbox_id():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    intent = _intent(action)
    bundle = DecisionBundle(
        decision_id=decision_id,
        decision_input=decision_input,
        kernel_version="kernel-v1",
        status=DecisionBundleStatus.PAPER_ACTIONABLE,
        forecasts=(forecast,),
        actions=(action,),
        execution_intents=(intent,),
    )
    receipt = DecisionCommitReceipt(
        decision_id=action.decision_id,
        result_hash=bundle.result_hash,
        status=CommitStatus.COMMITTED,
        committed_at=DECISION_TIME,
    )
    message = CommittedExecutionIntent(
        bundle=bundle,
        intent=intent,
        commit_receipt=receipt,
        outbox_id="",
    )
    assert message.outbox_id.startswith("outbox_")

    with pytest.raises(ValueError, match="outbox_id"):
        CommittedExecutionIntent(
            bundle=bundle,
            intent=intent,
            commit_receipt=receipt,
            outbox_id="forged-outbox",
        )

    forged_intent = replace(intent, limit_price=Decimal("25.11"), idempotency_key="")
    with pytest.raises(ValueError, match="not a member"):
        CommittedExecutionIntent(
            bundle=bundle,
            intent=forged_intent,
            commit_receipt=receipt,
            outbox_id="",
        )

    wrong_receipt = replace(receipt, result_hash="9" * 64)
    with pytest.raises(ValueError, match="result_hash does not match"):
        CommittedExecutionIntent(
            bundle=bundle,
            intent=intent,
            commit_receipt=wrong_receipt,
            outbox_id="",
        )

    class DecisionBundleSubclass(DecisionBundle):
        pass

    subclass_bundle = DecisionBundleSubclass(
        decision_id=decision_id,
        decision_input=decision_input,
        kernel_version="kernel-v1",
        status=DecisionBundleStatus.PAPER_ACTIONABLE,
        forecasts=(forecast,),
        actions=(action,),
        execution_intents=(intent,),
    )
    subclass_receipt = replace(
        receipt,
        result_hash=subclass_bundle.result_hash,
    )
    with pytest.raises(TypeError, match="exactly DecisionBundle"):
        CommittedExecutionIntent(
            bundle=subclass_bundle,
            intent=intent,
            commit_receipt=subclass_receipt,
            outbox_id="",
        )


def test_decision_bundle_result_hash_is_deterministic():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    intent = _intent(action)
    first = DecisionBundle(
        decision_id=decision_id,
        decision_input=decision_input,
        kernel_version="kernel-v1",
        status=DecisionBundleStatus.PAPER_ACTIONABLE,
        forecasts=(forecast,),
        actions=(action,),
        execution_intents=(intent,),
        diagnostics={"gates": {"risk": "PASS", "execution": "PASS"}},
    )
    second = DecisionBundle(
        decision_id=decision_id,
        decision_input=decision_input,
        kernel_version="kernel-v1",
        status=DecisionBundleStatus.PAPER_ACTIONABLE,
        forecasts=(forecast,),
        execution_intents=(intent,),
        actions=(action,),
        diagnostics={"gates": {"execution": "PASS", "risk": "PASS"}},
    )
    assert first.result_hash == second.result_hash
    assert first.as_dict()["result_hash"] == first.result_hash


def test_decision_bundle_identity_is_derived_from_input():
    decision_input = _decision_input()
    with pytest.raises(ValueError, match="decision_id"):
        DecisionBundle(
            decision_id="forged-decision",
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.RESEARCH_ONLY,
        )


def test_bundle_rejects_action_before_decision_and_after_forecast_expiry():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    early_action = DecisionAction(
        decision_id=decision_id,
        instrument="600001.SH",
        desired_action=ActionType.ADD,
        executable_action=ActionType.ADD,
        current_quantity=1000,
        sellable_quantity=0,
        target_quantity=1100,
        earliest_execution_time=DECISION_TIME - timedelta(seconds=1),
        valid_until=DECISION_TIME + timedelta(minutes=5),
        candidate_status=CandidateStatus.PAPER_ACTIONABLE,
        forecast_ids=(forecast.forecast_id,),
    )
    with pytest.raises(ValueError, match="starts before decision_time"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(early_action,),
            execution_intents=(_intent(early_action),),
        )

    short_forecast = replace(
        forecast,
        valid_until=DECISION_TIME + timedelta(minutes=2),
    )
    action = _action(
        decision_id,
        forecast_ids=(short_forecast.forecast_id,),
    )
    with pytest.raises(ValueError, match="outlives a referenced forecast"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(short_forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )


def test_bundle_rejects_forecast_created_before_model_or_calibration_available():
    decision_input = _decision_input()
    early_signal = CUTOFF - timedelta(days=2)
    forecast = replace(_forecast(), signal_at=early_signal)
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    with pytest.raises(ValueError, match="predates model availability"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )

    context = replace(
        decision_input.context,
        model_available_at={
            "stock": early_signal - timedelta(seconds=1),
            "market": CUTOFF - timedelta(days=1),
        },
    )
    decision_input = replace(decision_input, context=context)
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    with pytest.raises(ValueError, match="predates calibration availability"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )


def test_bundle_rejects_blocked_or_wrong_scope_forecast_for_add():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    blocked_forecast = replace(
        _forecast(),
        expected_return_net_pct=None,
        cvar95_loss_pct=None,
        probability_positive=None,
        status=CandidateStatus.DATA_BLOCKED,
    )
    action = _action(
        decision_id,
        forecast_ids=(blocked_forecast.forecast_id,),
    )
    with pytest.raises(ValueError, match="complete PAPER_ACTIONABLE forecast"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(blocked_forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )

    market_forecast = replace(
        _forecast(),
        scope=ScopeRef(ScopeType.MARKET, "CN_A"),
    )
    action = _action(
        decision_id,
        forecast_ids=(market_forecast.forecast_id,),
    )
    with pytest.raises(ValueError, match="same-instrument forecast"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(market_forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )

    primary_forecast = _forecast()
    blocked_market_forecast = replace(
        primary_forecast,
        scope=ScopeRef(ScopeType.MARKET, "CN_A"),
        expected_return_net_pct=None,
        cvar95_loss_pct=None,
        probability_positive=None,
        status=CandidateStatus.DATA_BLOCKED,
    )
    action = _action(
        decision_id,
        forecast_ids=(
            primary_forecast.forecast_id,
            blocked_market_forecast.forecast_id,
        ),
    )
    with pytest.raises(ValueError, match="every referenced forecast"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(primary_forecast, blocked_market_forecast),
            actions=(action,),
            execution_intents=(_intent(action),),
        )


def test_bundle_enforces_buy_lot_and_limit_price_rules():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    odd_lot_action = DecisionAction(
        decision_id=decision_id,
        instrument="600001.SH",
        desired_action=ActionType.ADD,
        executable_action=ActionType.ADD,
        current_quantity=1000,
        sellable_quantity=0,
        target_quantity=1050,
        earliest_execution_time=DECISION_TIME + timedelta(seconds=1),
        valid_until=DECISION_TIME + timedelta(minutes=5),
        candidate_status=CandidateStatus.PAPER_ACTIONABLE,
        forecast_ids=(forecast.forecast_id,),
    )
    with pytest.raises(ValueError, match="buy_lot_size"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(odd_lot_action,),
        )

    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    intent = _intent(action)
    missing_limit = replace(
        intent,
        limit_policy=LimitPolicy.FIXED_LIMIT,
        limit_price=None,
        idempotency_key="",
    )
    with pytest.raises(ValueError, match="requires limit_price"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(missing_limit,),
        )

    off_tick = replace(
        intent,
        limit_price=Decimal("25.105"),
        idempotency_key="",
    )
    with pytest.raises(ValueError, match="not tick aligned"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(off_tick,),
        )


def test_rule_snapshot_must_cover_decision_and_full_action_window():
    decision_input = _decision_input()
    expired_rule = replace(
        decision_input.instrument_rules[0],
        valid_until=DECISION_TIME - timedelta(microseconds=1),
    )
    with pytest.raises(ValueError, match="expired before decision_time"):
        replace(decision_input, instrument_rules=(expired_rule,))

    short_rule = replace(
        decision_input.instrument_rules[0],
        valid_until=DECISION_TIME + timedelta(minutes=2),
    )
    decision_input = replace(
        decision_input,
        instrument_rules=(short_rule,),
    )
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    with pytest.raises(ValueError, match="outlives its instrument rule"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )


def test_bundle_enforces_sell_lot_with_exact_odd_remainder_exception():
    decision_input = _decision_input()
    position = replace(
        decision_input.account.positions[0],
        sellable_quantity=1000,
    )
    account = replace(decision_input.account, positions=(position,))
    rule = replace(
        decision_input.instrument_rules[0],
        sell_lot_size=100,
        allow_odd_lot_liquidation=False,
    )
    decision_input = replace(
        decision_input,
        account=account,
        instrument_rules=(rule,),
    )
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    action = DecisionAction(
        decision_id=decision_id,
        instrument="600001.SH",
        desired_action=ActionType.REDUCE,
        executable_action=ActionType.REDUCE,
        current_quantity=1000,
        sellable_quantity=1000,
        target_quantity=950,
        earliest_execution_time=DECISION_TIME + timedelta(seconds=1),
        valid_until=DECISION_TIME + timedelta(minutes=5),
        candidate_status=CandidateStatus.PAPER_ACTIONABLE,
    )
    intent = ExecutionIntent(
        strategy_id="trading-v4-clean",
        decision_id=decision_id,
        action_id=action.action_id,
        account_id=account.account_id,
        instrument=action.instrument,
        side=ExecutionSide.SELL,
        desired_quantity=50,
        target_quantity=950,
        limit_policy=LimitPolicy.MARKETABLE_LIMIT,
        earliest_at=action.earliest_execution_time,
        valid_until=action.valid_until,
        execution_contract_version="execution-v1",
        limit_price=Decimal("25.10"),
    )
    with pytest.raises(ValueError, match="sell_lot_size"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            actions=(action,),
            execution_intents=(intent,),
        )

    odd_position = replace(
        position,
        total_quantity=1050,
        sellable_quantity=1050,
    )
    odd_account = replace(account, positions=(odd_position,))
    odd_rule = replace(rule, allow_odd_lot_liquidation=True)
    odd_input = replace(
        decision_input,
        account=odd_account,
        instrument_rules=(odd_rule,),
    )
    odd_decision_id = derive_decision_id(odd_input, "kernel-v1")
    odd_action = DecisionAction(
        decision_id=odd_decision_id,
        instrument="600001.SH",
        desired_action=ActionType.REDUCE,
        executable_action=ActionType.REDUCE,
        current_quantity=1050,
        sellable_quantity=1050,
        target_quantity=1000,
        earliest_execution_time=DECISION_TIME + timedelta(seconds=1),
        valid_until=DECISION_TIME + timedelta(minutes=5),
        candidate_status=CandidateStatus.PAPER_ACTIONABLE,
    )
    odd_intent = replace(
        intent,
        decision_id=odd_decision_id,
        action_id=odd_action.action_id,
        desired_quantity=50,
        target_quantity=1000,
        idempotency_key="",
    )
    bundle = DecisionBundle(
        decision_id=odd_decision_id,
        decision_input=odd_input,
        kernel_version="kernel-v1",
        status=DecisionBundleStatus.PAPER_ACTIONABLE,
        actions=(odd_action,),
        execution_intents=(odd_intent,),
    )
    assert bundle.execution_intents == (odd_intent,)


def test_bundle_rejects_execution_intent_without_its_action():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    action = _action(decision_id)
    with pytest.raises(ValueError, match="does not reference"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            execution_intents=(_intent(action),),
        )


def test_non_actionable_bundle_cannot_smuggle_an_execution_intent():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    with pytest.raises(ValueError, match="PAPER_ACTIONABLE"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.RESEARCH_ONLY,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(_intent(action),),
        )


def test_execution_intent_must_match_action_side_and_quantity():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    original = _intent(action)
    wrong_side = ExecutionIntent(
        strategy_id=original.strategy_id,
        decision_id=original.decision_id,
        action_id=original.action_id,
        account_id=original.account_id,
        instrument=original.instrument,
        side=ExecutionSide.SELL,
        desired_quantity=original.desired_quantity,
        target_quantity=original.target_quantity,
        limit_policy=original.limit_policy,
        earliest_at=original.earliest_at,
        valid_until=original.valid_until,
        execution_contract_version=original.execution_contract_version,
    )

    with pytest.raises(ValueError, match="side does not match"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(wrong_side,),
        )

    wrong_quantity = ExecutionIntent(
        strategy_id=original.strategy_id,
        decision_id=original.decision_id,
        action_id=original.action_id,
        account_id=original.account_id,
        instrument=original.instrument,
        side=original.side,
        desired_quantity=200,
        target_quantity=original.target_quantity,
        limit_policy=original.limit_policy,
        earliest_at=original.earliest_at,
        valid_until=original.valid_until,
        execution_contract_version=original.execution_contract_version,
    )
    with pytest.raises(ValueError, match="quantity does not match"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(wrong_quantity,),
        )


def test_execution_intent_must_match_action_instrument_and_context_version():
    decision_input = _decision_input()
    decision_id = derive_decision_id(decision_input, "kernel-v1")
    forecast = _forecast()
    action = _action(decision_id, forecast_ids=(forecast.forecast_id,))
    original = _intent(action)
    wrong_instrument = ExecutionIntent(
        strategy_id=original.strategy_id,
        decision_id=original.decision_id,
        action_id=original.action_id,
        account_id=original.account_id,
        instrument="000001.SZ",
        side=original.side,
        desired_quantity=original.desired_quantity,
        target_quantity=original.target_quantity,
        limit_policy=original.limit_policy,
        earliest_at=original.earliest_at,
        valid_until=original.valid_until,
        execution_contract_version=original.execution_contract_version,
    )
    with pytest.raises(ValueError, match="instrument does not match"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(wrong_instrument,),
        )

    wrong_version = ExecutionIntent(
        strategy_id=original.strategy_id,
        decision_id=original.decision_id,
        action_id=original.action_id,
        account_id=original.account_id,
        instrument=original.instrument,
        side=original.side,
        desired_quantity=original.desired_quantity,
        target_quantity=original.target_quantity,
        limit_policy=original.limit_policy,
        earliest_at=original.earliest_at,
        valid_until=original.valid_until,
        execution_contract_version="execution-v2",
    )
    with pytest.raises(ValueError, match="contract version"):
        DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version="kernel-v1",
            status=DecisionBundleStatus.PAPER_ACTIONABLE,
            forecasts=(forecast,),
            actions=(action,),
            execution_intents=(wrong_version,),
        )


def test_blocked_kernel_is_deterministic_and_never_emits_actions():
    decision_input = _decision_input()
    kernel = BlockedDecisionKernel()

    assert isinstance(kernel, DecisionKernel)
    first = kernel.evaluate(decision_input)
    second = kernel.evaluate(decision_input)

    assert first == second
    assert first.result_hash == second.result_hash
    assert first.decision_id == derive_decision_id(
        decision_input,
        kernel.kernel_version,
    )
    assert first.status is DecisionBundleStatus.DATA_BLOCKED
    assert first.forecasts == ()
    assert first.actions == ()
    assert first.execution_intents == ()
    assert first.diagnostics["production_activation_allowed"] is False
    assert first.diagnostics["actionable_output_allowed"] is False
    assert first.diagnostics["paper_buy_outbox_open"] is False
    assert first.kernel_version == "v4:kernel:blocked:v1"
    assert first.diagnostics["reason_codes"] == (
        "ACTIONABLE_OUTPUT_DISABLED",
        "DATA_UNAVAILABLE",
        "SAFETY_INTERLOCK",
        "STAGE_3_NOT_AUTHORIZED",
    )


def test_blocked_kernel_configuration_and_input_fail_closed():
    with pytest.raises(TypeError, match="kernel_version"):
        BlockedDecisionKernel(
            kernel_version="v4:kernel:paper-actionable:v1"  # type: ignore[call-arg]
        )
    with pytest.raises(TypeError, match="reason_codes"):
        BlockedDecisionKernel(
            reason_codes=("PRODUCTION_READY",)  # type: ignore[call-arg]
        )

    kernel = BlockedDecisionKernel()
    with pytest.raises(TypeError, match="exactly DecisionInput"):
        kernel.evaluate(object())  # type: ignore[arg-type]
    with pytest.raises(FrozenInstanceError):
        kernel.kernel_version = "v4:kernel:other:v1"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        kernel.reason_codes = ("PRODUCTION_READY",)  # type: ignore[misc]
