"""Pure coordinator for an explicit forward-only V4 research snapshot."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

from ..domain import (
    AccountSnapshot,
    AsOfDataset,
    AvailabilityStatus,
    CapabilityStatus,
    DataManifest,
    DecisionBundle,
    DecisionClock,
    DecisionContext,
    DecisionInput,
    InstrumentRuleSnapshot,
    QualityStatus,
    ResearchStatus,
    SourceWatermark,
    deterministic_hash,
    deterministic_id,
)
from ..factors import build_chase_risk_feature_vector
from ..kernel import ResearchDecisionKernel
from .validation import validate_research_observation_bundle


SHANGHAI_OFFSET = timedelta(hours=8)


def build_forward_research_decision_input(
    dataset: AsOfDataset,
    *,
    instruments: tuple[str, ...],
    decision_time: datetime,
    valid_until: datetime,
    decision_clock: DecisionClock,
    code_commit_sha: str,
    config_hash: str,
) -> DecisionInput:
    """Build one V4-only DecisionInput from explicit forward evidence.

    The coordinator intentionally classifies the source capability as
    ``FORWARD_ONLY``.  A caller cannot promote it to PIT-certified or
    backtest-ready through a command-line flag.
    """

    if type(dataset) is not AsOfDataset:
        raise TypeError("dataset must be exactly AsOfDataset")
    if DecisionClock(decision_clock) != DecisionClock.AFTER_CLOSE:
        raise ValueError("V4.1 forward daily-bar research requires AFTER_CLOSE")
    _require_aware(decision_time, "decision_time")
    _require_aware(valid_until, "valid_until")
    if decision_time.utcoffset() != SHANGHAI_OFFSET:
        raise ValueError(
            "V4.1 A-share AFTER_CLOSE decision_time must use +08:00"
        )
    if (decision_time.hour, decision_time.minute, decision_time.second) < (15, 0, 0):
        raise ValueError(
            "V4.1 AFTER_CLOSE decision_time must be at or after 15:00 "
            "in its explicit offset"
        )
    if dataset.as_of.utcoffset() != decision_time.utcoffset():
        raise ValueError(
            "dataset.as_of and decision_time must use the same explicit offset"
        )
    if valid_until.utcoffset() != decision_time.utcoffset():
        raise ValueError(
            "valid_until and decision_time must use the same explicit offset"
        )
    if dataset.as_of != decision_time:
        raise ValueError(
            "V4.1 forward research requires decision_time to equal dataset.as_of"
        )
    if valid_until < decision_time:
        raise ValueError("valid_until cannot precede decision_time")
    if valid_until > decision_time + timedelta(hours=24):
        raise ValueError("V4.1 forward research validity cannot exceed 24 hours")
    normalized_instruments = tuple(
        sorted({_required_text(item, "instrument") for item in instruments})
    )
    if not normalized_instruments:
        raise ValueError("instruments must not be empty")
    _validate_explicit_daily_bar_records(
        dataset,
        normalized_instruments,
        decision_time=decision_time,
    )

    record_hashes: dict[str, str] = {}
    for record in dataset.records:
        existing = record_hashes.get(record.record_id)
        if existing is not None and existing != record.record_hash:
            raise ValueError(
                "V4 research input requires globally unique record_id values"
            )
        record_hashes[record.record_id] = record.record_hash
    manifest = DataManifest(record_hashes=record_hashes)

    features = tuple(
        build_chase_risk_feature_vector(
            dataset,
            instrument=instrument,
            cutoff=dataset.as_of,
            valid_until=valid_until,
            data_manifest=manifest,
        )
        for instrument in normalized_instruments
    )

    capability_quality = _combined_quality(
        dataset.quality_status,
        *(record.quality_status for record in dataset.records),
    )
    capability = CapabilityStatus(
        name="daily_bar_chase_risk",
        availability_status=(
            AvailabilityStatus.ACTIVE
            if capability_quality == QualityStatus.PASS
            else AvailabilityStatus.DEGRADED
        ),
        research_status=ResearchStatus.FORWARD_ONLY,
        quality_status=capability_quality,
        reason_codes=(
            "CALLER_SUPPLIED_FORWARD_DATA",
            "NOT_PIT_CERTIFIED",
            "SOURCE_AUTHENTICITY_NOT_CERTIFIED",
        ),
    )
    watermarks = _source_watermarks(dataset, valid_until=valid_until)
    account_snapshot_id = deterministic_id(
        "v4researchaccount",
        {
            "dataset_id": dataset.dataset_id,
            "decision_time": decision_time,
            "instruments": normalized_instruments,
        },
    )
    context = DecisionContext(
        decision_time=decision_time,
        decision_clock=decision_clock,
        knowledge_cutoff=dataset.as_of,
        trade_date=decision_time.date(),
        universe_version=f"v4:forward-universe:{dataset.dataset_id}",
        data_manifest=manifest,
        portfolio_policy_version="v4:research-no-portfolio:v1",
        execution_contract_version="v4:no-execution:v1",
        fee_schedule_version="v4:no-fee-estimate:v1",
        account_snapshot_id=account_snapshot_id,
        code_commit_sha=code_commit_sha,
        config_hash=config_hash,
        random_seed=0,
        source_watermarks=watermarks,
        factor_spec_versions={
            "daily_bar_chase_risk": "v4:daily-bar-chase-risk-v2",
            "transparent_screen": "v4:transparent-screening-policy:v1",
        },
        capability_statuses={capability.name: capability},
    )
    account = AccountSnapshot(
        account_snapshot_id=account_snapshot_id,
        account_id="v4-research-no-execution",
        as_of=dataset.as_of,
        available_cash=Decimal("0"),
        equity=Decimal("0"),
    )
    rules = tuple(
        InstrumentRuleSnapshot(
            instrument=instrument,
            rule_version="v4:research-non-execution-rule:v1",
            effective_at=dataset.as_of,
            knowledge_time=dataset.as_of,
            valid_until=valid_until,
            can_buy=False,
            can_sell=False,
            first_buy_minimum=0,
            buy_lot_size=100,
            sell_lot_size=1,
            settlement_days=1,
            tick_size=Decimal("0.01"),
            allow_odd_lot_liquidation=False,
            quality_status=QualityStatus.PASS,
        )
        for instrument in normalized_instruments
    )
    return DecisionInput(
        context=context,
        account=account,
        scopes=tuple(feature.scope for feature in features),
        feature_vectors=features,
        instrument_rules=rules,
    )


def run_forward_research_observation(
    dataset: AsOfDataset,
    *,
    instruments: tuple[str, ...],
    decision_time: datetime,
    valid_until: datetime,
    decision_clock: DecisionClock,
    code_commit_sha: str,
    config_hash: str,
) -> DecisionBundle:
    """Build, evaluate and strictly validate one V4 research observation."""

    decision_input = build_forward_research_decision_input(
        dataset,
        instruments=instruments,
        decision_time=decision_time,
        valid_until=valid_until,
        decision_clock=decision_clock,
        code_commit_sha=code_commit_sha,
        config_hash=config_hash,
    )
    bundle = ResearchDecisionKernel().evaluate(decision_input)
    return validate_research_observation_bundle(bundle)


def _source_watermarks(
    dataset: AsOfDataset,
    *,
    valid_until: datetime,
) -> dict[str, SourceWatermark]:
    grouped: dict[str, list] = defaultdict(list)
    for record in dataset.records:
        grouped[record.source].append(record)
    watermarks: dict[str, SourceWatermark] = {}
    for source, records in sorted(grouped.items()):
        quality = _combined_quality(
            dataset.quality_status,
            *(record.quality_status for record in records),
        )
        record_hashes = tuple(sorted(record.record_hash for record in records))
        watermarks[source] = SourceWatermark(
            source=source,
            knowledge_time=max(record.knowledge_time for record in records),
            record_count=len(records),
            quality_status=quality,
            snapshot_id=deterministic_id(
                "v4snapshot",
                {"source": source, "record_hashes": record_hashes},
            ),
            valid_until=valid_until,
            coverage=Decimal("1"),
            batch_id=dataset.dataset_id,
            schema_version="v4:explicit-forward-records:v1",
            content_hash=deterministic_hash(record_hashes),
            reason_codes=("NOT_PIT_CERTIFIED",),
        )
    return watermarks


def _combined_quality(*values: QualityStatus) -> QualityStatus:
    normalized = tuple(QualityStatus(value) for value in values)
    if QualityStatus.FAIL in normalized:
        return QualityStatus.FAIL
    if QualityStatus.WARN in normalized:
        return QualityStatus.WARN
    return QualityStatus.PASS


def _validate_explicit_daily_bar_records(
    dataset: AsOfDataset,
    instruments: tuple[str, ...],
    *,
    decision_time: datetime,
) -> None:
    selected = set(instruments)
    seen: set[str] = set()
    selected_records: dict[str, list] = defaultdict(list)
    for record in dataset.records:
        for field_name in (
            "knowledge_time",
            "ingested_at",
            "event_time",
            "source_published_at",
            "first_seen_at",
            "received_at",
            "revised_at",
        ):
            timestamp = getattr(record, field_name)
            if (
                timestamp is not None
                and timestamp.utcoffset() != decision_time.utcoffset()
            ):
                raise ValueError(
                    f"daily-bar {field_name} must use the decision_time offset"
                )
        instrument = record.payload.get("instrument")
        if instrument not in selected:
            continue
        seen.add(instrument)
        selected_records[instrument].append(record)
        if record.event_time is None:
            raise ValueError(
                "explicit forward daily bars require event_time; missing values "
                "cannot be used to infer a completed session"
            )
        if record.event_time > record.knowledge_time:
            raise ValueError("daily-bar event_time cannot exceed knowledge_time")
        trade_date_value = record.payload.get("trade_date")
        if not isinstance(trade_date_value, str):
            raise ValueError("daily-bar trade_date must be an ISO date")
        try:
            trade_date = date.fromisoformat(trade_date_value)
        except ValueError as exc:
            raise ValueError("daily-bar trade_date must be an ISO date") from exc
        if trade_date != record.event_time.date():
            raise ValueError(
                "daily-bar trade_date must match the explicit event_time date"
            )
    missing = selected - seen
    if missing:
        raise ValueError(
            "no explicit daily-bar records found for instruments: "
            f"{tuple(sorted(missing))}"
        )
    latest_selected_knowledge = max(
        record.knowledge_time
        for records in selected_records.values()
        for record in records
    )
    if dataset.as_of != latest_selected_knowledge:
        raise ValueError(
            "dataset.as_of must equal the latest selected record knowledge_time"
        )
    for instrument, records in selected_records.items():
        latest = max(records, key=lambda item: item.knowledge_time)
        if dataset.as_of - latest.knowledge_time > timedelta(hours=6):
            raise ValueError(
                f"latest record for {instrument} exceeds the 6-hour freshness SLA"
            )
        if latest.event_time is None or latest.event_time.date() != decision_time.date():
            raise ValueError(
                f"latest completed session for {instrument} is not the decision date"
            )


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "build_forward_research_decision_input",
    "run_forward_research_observation",
]
