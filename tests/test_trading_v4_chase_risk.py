from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.trading_v4.domain import (
    AccountSnapshot,
    AsOfDataset,
    AsOfRecord,
    AvailabilityStatus,
    CandidateStatus,
    CapabilityStatus,
    DataManifest,
    DecisionClock,
    DecisionContext,
    DecisionInput,
    FeatureVector,
    InstrumentRuleSnapshot,
    QualityStatus,
    ResearchStatus,
)
from server.trading_v4.factors import (
    assess_chase_risk,
    build_chase_risk_feature_vector,
)


UTC = timezone.utc
START = datetime(2026, 6, 1, 15, 0, tzinfo=UTC)
INSTRUMENT = "603221.SH"


def _record(
    index: int,
    *,
    previous_close: Decimal,
    close: Decimal,
    upper_limit: Decimal | None,
    volume: Decimal = Decimal("1000000"),
    open_price: Decimal | None = None,
    high: Decimal | None = None,
    low: Decimal | None = None,
    turnover_pct: Decimal | None = Decimal("10"),
    suspended: bool = False,
    capacity: Decimal | bool | None = None,
) -> AsOfRecord:
    event_time = START + timedelta(days=index)
    actual_open = open_price if open_price is not None else previous_close
    actual_high = high if high is not None else max(actual_open, close)
    actual_low = low if low is not None else min(actual_open, close)
    payload: dict[str, object] = {
        "instrument": INSTRUMENT,
        "trade_date": event_time.date().isoformat(),
        "open": actual_open,
        "high": actual_high,
        "low": actual_low,
        "close": close,
        "previous_close": previous_close,
        "volume": volume,
        "amount": volume * close,
        "turnover_pct": turnover_pct,
        "is_suspended": suspended,
    }
    if upper_limit is not None:
        payload["upper_limit"] = upper_limit
    if capacity is not None:
        payload["verified_capacity"] = capacity
    return AsOfRecord(
        record_id=f"bar-{index:03d}",
        source="exchange_daily_bar",
        event_time=event_time,
        knowledge_time=event_time + timedelta(minutes=10),
        ingested_at=event_time + timedelta(minutes=5),
        payload=payload,
    )


def _dataset(
    records: list[AsOfRecord],
    *,
    as_of: datetime | None = None,
) -> AsOfDataset:
    return AsOfDataset(
        dataset_name="exchange-daily-bars",
        as_of=as_of or max(item.knowledge_time for item in records),
        records=tuple(records),
        quality_status=QualityStatus.PASS,
    )


def _base_and_limit_streak(count: int) -> list[AsOfRecord]:
    records = [
        _record(
            0,
            previous_close=Decimal("9.90"),
            close=Decimal("10.00"),
            upper_limit=Decimal("10.89"),
        )
    ]
    previous = Decimal("10.00")
    for index in range(1, count + 1):
        close = previous * Decimal("1.10")
        records.append(
            _record(
                index,
                previous_close=previous,
                open_price=previous * Decimal("1.02"),
                low=previous * Decimal("1.01"),
                high=close,
                close=close,
                upper_limit=close,
            )
        )
        previous = close
    return records


def test_nine_limit_streak_then_two_zero_volume_days_is_execution_blocked():
    records = _base_and_limit_streak(9)
    previous = Decimal(records[-1].payload["close"])
    records.extend(
        [
            _record(
                10,
                previous_close=previous,
                close=previous,
                upper_limit=None,
                volume=Decimal("0"),
                suspended=True,
            ),
            _record(
                11,
                previous_close=previous,
                close=previous,
                upper_limit=None,
                volume=Decimal("0"),
                suspended=True,
            ),
        ]
    )

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.limit_streak == 9
    assert result.surge_streak == 9
    assert result.zero_volume is True
    assert result.no_capacity is True
    assert result.has_verified_capacity is False
    assert result.extreme_extension is True
    assert result.ordinary_buy_eligible is False
    assert result.candidate_status == CandidateStatus.EXECUTION_BLOCKED
    assert "NO_VERIFIED_CAPACITY" in result.reason_codes


def test_three_boards_are_conditional_and_four_plus_are_watch_only():
    three = assess_chase_risk(
        _dataset(_base_and_limit_streak(3)),
        instrument=INSTRUMENT,
    )
    four = assess_chase_risk(
        _dataset(_base_and_limit_streak(4)),
        instrument=INSTRUMENT,
    )

    assert three.limit_streak == 3
    assert three.candidate_status == CandidateStatus.CONDITIONAL
    assert three.ordinary_buy_eligible is False
    assert four.limit_streak == 4
    assert four.extreme_extension is True
    assert four.candidate_status == CandidateStatus.WATCH
    assert four.ordinary_buy_eligible is False


def test_nine_board_peak_survives_one_ordinary_day_until_explicit_rebase():
    records = _base_and_limit_streak(9)
    previous = Decimal(records[-1].payload["close"])
    ordinary_close = previous * Decimal("1.01")
    records.append(
        _record(
            10,
            previous_close=previous,
            close=ordinary_close,
            upper_limit=previous * Decimal("1.10"),
            high=ordinary_close + Decimal("0.1"),
            low=previous,
        )
    )

    assessment = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert assessment.surge_streak == 0
    assert assessment.limit_streak == 0
    assert assessment.peak_streak == 9
    assert assessment.sessions_since_peak == 1
    assert assessment.cooldown_active is True
    assert assessment.candidate_status == CandidateStatus.WATCH
    assert "PEAK_STREAK_COOLDOWN" in assessment.reason_codes


def test_peak_risk_recovers_only_after_a_full_cooldown_and_rebase_window():
    records = _base_and_limit_streak(9)
    previous = Decimal(records[-1].payload["close"])
    for index in range(10, 30):
        records.append(
            _record(
                index,
                previous_close=previous,
                close=previous,
                upper_limit=previous * Decimal("1.10"),
                high=previous + Decimal("0.1"),
                low=previous - Decimal("0.1"),
            )
        )

    assessment = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert assessment.peak_streak == 9
    assert assessment.sessions_since_peak == 20
    assert assessment.cooldown_active is False
    assert assessment.extreme_extension is False
    assert assessment.candidate_status == CandidateStatus.RESEARCH_ONLY
    assert assessment.ordinary_buy_eligible is True


def test_sufficient_drawdown_explicitly_rebases_peak_cooldown() -> None:
    records = _base_and_limit_streak(9)
    peak = Decimal(records[-1].payload["close"])
    rebased_close = peak * Decimal("0.85")
    records.append(
        _record(
            10,
            previous_close=peak,
            close=rebased_close,
            upper_limit=peak * Decimal("1.10"),
            high=peak,
            low=rebased_close - Decimal("0.1"),
        )
    )

    assessment = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert assessment.drawdown_from_peak_pct == Decimal("15.000000")
    assert assessment.cooldown_active is False
    assert assessment.candidate_status != CandidateStatus.WATCH


def test_missing_limit_rules_are_none_while_conservative_surge_still_blocks():
    records = _base_and_limit_streak(4)
    without_rules: list[AsOfRecord] = []
    for record in records:
        payload = dict(record.payload)
        payload.pop("upper_limit", None)
        without_rules.append(
            AsOfRecord(
                record_id=record.record_id,
                source=record.source,
                event_time=record.event_time,
                knowledge_time=record.knowledge_time,
                ingested_at=record.ingested_at,
                payload=payload,
            )
        )

    result = assess_chase_risk(_dataset(without_rules), instrument=INSTRUMENT)

    assert result.limit_streak is None
    assert result.surge_streak == 4
    assert result.candidate_status == CandidateStatus.WATCH
    assert result.ordinary_buy_eligible is False
    assert "LIMIT_RULE_MISSING" in result.reason_codes


def test_explicit_zero_capacity_overrides_other_candidate_states():
    records = _base_and_limit_streak(3)
    previous = records[-1]
    payload = dict(previous.payload)
    payload["verified_capacity"] = Decimal("0")
    records[-1] = AsOfRecord(
        record_id=previous.record_id,
        source=previous.source,
        event_time=previous.event_time,
        knowledge_time=previous.knowledge_time,
        ingested_at=previous.ingested_at,
        payload=payload,
    )

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.zero_volume is False
    assert result.no_capacity is True
    assert result.candidate_status == CandidateStatus.EXECUTION_BLOCKED
    assert result.ordinary_buy_eligible is False


@pytest.mark.parametrize(
    ("amount", "expected_status", "missing"),
    [
        (None, CandidateStatus.DATA_BLOCKED, True),
        (Decimal("0"), CandidateStatus.EXECUTION_BLOCKED, False),
    ],
)
def test_explicit_capacity_true_cannot_override_hard_amount_evidence(
    amount: Decimal | None,
    expected_status: CandidateStatus,
    missing: bool,
) -> None:
    records = _base_and_limit_streak(1)
    latest = records[-1]
    payload = dict(latest.payload)
    if amount is None:
        payload.pop("amount")
    else:
        payload["amount"] = amount
    payload["verified_capacity"] = True
    records[-1] = AsOfRecord(
        record_id=latest.record_id,
        source=latest.source,
        event_time=latest.event_time,
        knowledge_time=latest.knowledge_time,
        ingested_at=latest.ingested_at,
        payload=payload,
    )

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.has_verified_capacity is False
    assert result.no_capacity is True
    assert result.ordinary_buy_eligible is False
    assert result.candidate_status == expected_status
    assert ("amount" in result.missing_fields) is missing


def test_recent_three_streak_restarts_cooldown_after_older_nine_peak() -> None:
    records = _base_and_limit_streak(9)
    previous = Decimal(records[-1].payload["close"])
    next_index = 10
    for _ in range(11):
        records.append(
            _record(
                next_index,
                previous_close=previous,
                close=previous,
                upper_limit=previous * Decimal("1.10"),
            )
        )
        next_index += 1
    for _ in range(3):
        close = previous * Decimal("1.10")
        records.append(
            _record(
                next_index,
                previous_close=previous,
                close=close,
                upper_limit=close,
            )
        )
        previous = close
        next_index += 1
    records.append(
        _record(
            next_index,
            previous_close=previous,
            close=previous,
            upper_limit=previous * Decimal("1.10"),
        )
    )

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.peak_streak == 9
    assert result.recent_peak_streak == 3
    assert result.cooldown_active is True
    assert result.candidate_status in {
        CandidateStatus.CONDITIONAL,
        CandidateStatus.WATCH,
    }
    assert result.ordinary_buy_eligible is False


def test_returns_ma_and_atr_extensions_are_computed_from_daily_bars():
    records: list[AsOfRecord] = []
    previous = Decimal("100")
    for index in range(25):
        close = previous + Decimal("1")
        records.append(
            _record(
                index,
                previous_close=previous,
                open_price=previous + Decimal("0.25"),
                high=close + Decimal("1"),
                low=previous - Decimal("1"),
                close=close,
                upper_limit=previous * Decimal("1.10"),
                turnover_pct=Decimal("8"),
            )
        )
        previous = close

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.return_1d_pct == Decimal("0.806452")
    assert result.return_5d_pct == Decimal("4.166667")
    assert result.return_20d_pct == Decimal("19.047619")
    assert result.ma5 == Decimal("123")
    assert result.ma20 == Decimal("115.5")
    assert result.atr14 == Decimal("3")
    assert result.ma5_extension_pct == Decimal("1.626016")
    assert result.ma20_extension_pct == Decimal("8.225108")
    assert result.atr14_pct == Decimal("2.400000")
    assert result.ma5_extension_atr == Decimal("0.666667")
    assert result.ma20_extension_atr == Decimal("3.166667")
    assert result.extreme_extension is False
    assert result.ordinary_buy_eligible is True
    assert result.candidate_status == CandidateStatus.RESEARCH_ONLY
    assert result.quality_status == QualityStatus.PASS


def test_one_day_return_uses_authoritative_previous_close_not_previous_row():
    records = [
        _record(
            0,
            previous_close=Decimal("90"),
            close=Decimal("100"),
            upper_limit=Decimal("120"),
        ),
        _record(
            2,
            previous_close=Decimal("110"),
            close=Decimal("120"),
            upper_limit=Decimal("132"),
        ),
    ]

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.return_1d_pct == Decimal("9.090909")


def test_same_authority_conflicting_daily_bars_fail_closed():
    original = _record(
        0,
        previous_close=Decimal("100"),
        close=Decimal("105"),
        upper_limit=Decimal("110"),
    )
    original = AsOfRecord(
        record_id="bar-conflict-a",
        source=original.source,
        event_time=original.event_time,
        knowledge_time=original.knowledge_time,
        ingested_at=original.ingested_at,
        revision_id="revision-1",
        payload=original.payload,
    )
    conflicting_payload = dict(original.payload)
    conflicting_payload["close"] = Decimal("106")
    conflicting_payload["high"] = Decimal("106")
    conflict = AsOfRecord(
        record_id="bar-conflict-b",
        source=original.source,
        event_time=original.event_time,
        knowledge_time=original.knowledge_time,
        ingested_at=original.ingested_at,
        revision_id="revision-1",
        payload=conflicting_payload,
    )

    with pytest.raises(ValueError, match="conflicting daily bars"):
        assess_chase_risk(
            _dataset([original, conflict]),
            instrument=INSTRUMENT,
        )


def test_opaque_revision_ids_cannot_resolve_same_time_conflicts():
    first = _record(
        0,
        previous_close=Decimal("100"),
        close=Decimal("105"),
        upper_limit=Decimal("110"),
    )
    first = AsOfRecord(
        record_id="bar-revision-10",
        source=first.source,
        event_time=first.event_time,
        knowledge_time=first.knowledge_time,
        ingested_at=first.ingested_at,
        revision_id="10",
        payload=first.payload,
    )
    second_payload = dict(first.payload)
    second_payload["close"] = Decimal("106")
    second_payload["high"] = Decimal("106")
    second = AsOfRecord(
        record_id="bar-revision-9",
        source=first.source,
        event_time=first.event_time,
        knowledge_time=first.knowledge_time,
        ingested_at=first.ingested_at,
        revision_id="9",
        payload=second_payload,
    )

    with pytest.raises(ValueError, match="conflicting daily bars"):
        assess_chase_risk(_dataset([first, second]), instrument=INSTRUMENT)


def test_ma_atr_extension_is_an_explicit_watch_gate() -> None:
    records = [
        _record(
            index,
            previous_close=Decimal("100"),
            close=Decimal("100") if index < 19 else Decimal("130"),
            open_price=Decimal("100"),
            high=Decimal("101") if index < 19 else Decimal("131"),
            low=Decimal("99"),
            upper_limit=Decimal("200"),
        )
        for index in range(20)
    ]

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.surge_streak == 0
    assert result.ma20_extension_pct > Decimal("15")
    assert result.extreme_extension is True
    assert result.candidate_status == CandidateStatus.WATCH
    assert "MA_ATR_EXTREME_EXTENSION" in result.reason_codes


def test_return_gap_and_crowding_combination_marks_extreme_extension():
    records: list[AsOfRecord] = []
    previous = Decimal("100")
    for index in range(21):
        close = previous
        if index >= 16:
            close = previous * Decimal("1.08")
        open_price = previous * (Decimal("1.06") if index == 20 else Decimal("1"))
        records.append(
            _record(
                index,
                previous_close=previous,
                open_price=open_price,
                high=max(open_price, close) + Decimal("0.5"),
                low=min(open_price, close) - Decimal("0.5"),
                close=close,
                upper_limit=previous * Decimal("1.10"),
                turnover_pct=Decimal("25") if index == 20 else Decimal("10"),
            )
        )
        previous = close

    result = assess_chase_risk(_dataset(records), instrument=INSTRUMENT)

    assert result.return_5d_pct > Decimal("35")
    assert result.gap_pct == Decimal("6.000000")
    assert result.crowding_detected is True
    assert result.extreme_extension is True
    assert result.candidate_status == CandidateStatus.WATCH
    assert result.ordinary_buy_eligible is False


def test_future_tenth_board_cannot_change_nine_board_cutoff_feature():
    nine_records = _base_and_limit_streak(9)
    cutoff = nine_records[-1].knowledge_time
    valid_until = cutoff + timedelta(hours=2)
    base_dataset = _dataset(nine_records, as_of=cutoff)
    base_feature = build_chase_risk_feature_vector(
        base_dataset,
        instrument=INSTRUMENT,
        cutoff=cutoff,
        valid_until=valid_until,
    )

    future_records = _base_and_limit_streak(10)
    later_dataset = _dataset(future_records)
    replayed_feature = build_chase_risk_feature_vector(
        later_dataset,
        instrument=INSTRUMENT,
        cutoff=cutoff,
        valid_until=valid_until,
    )

    assert isinstance(base_feature, FeatureVector)
    assert base_feature.values["limit_streak"] == 9
    assert base_feature.values["ordinary_buy_eligible"] is False
    assert base_feature.values == replayed_feature.values
    assert base_feature.source_record_ids == replayed_feature.source_record_ids
    assert base_feature.source_manifest_hash == replayed_feature.source_manifest_hash
    assert base_feature.feature_hash == replayed_feature.feature_hash
    assert "bar-010" not in replayed_feature.source_record_ids


def test_one_price_limit_up_is_no_capacity_and_feature_is_traceable():
    records = _base_and_limit_streak(1)
    latest = records[-1]
    close = Decimal(latest.payload["close"])
    payload = dict(latest.payload)
    payload.update({"open": close, "high": close, "low": close})
    records[-1] = AsOfRecord(
        record_id=latest.record_id,
        source=latest.source,
        event_time=latest.event_time,
        knowledge_time=latest.knowledge_time,
        ingested_at=latest.ingested_at,
        payload=payload,
    )
    dataset = _dataset(records)

    feature = build_chase_risk_feature_vector(
        dataset,
        instrument=INSTRUMENT,
        valid_until=dataset.as_of + timedelta(hours=1),
    )

    assert feature.values["one_price_limit_up"] is True
    assert feature.values["no_capacity"] is True
    assert feature.values["candidate_status"] == "EXECUTION_BLOCKED"
    assert feature.values["ordinary_buy_eligible"] is False
    assert feature.source_record_ids == ("bar-000", "bar-001")
    assert set(feature.source_record_hashes) == set(feature.source_record_ids)


def test_chase_feature_uses_canonical_manifest_and_builds_decision_input() -> None:
    records: list[AsOfRecord] = []
    previous = Decimal("100")
    for index in range(25):
        close = previous + Decimal("1")
        records.append(
            _record(
                index,
                previous_close=previous,
                close=close,
                upper_limit=previous * Decimal("1.10"),
            )
        )
        previous = close
    dataset = _dataset(records)
    source_hashes = {item.record_id: item.record_hash for item in records}
    manifest = DataManifest(
        {
            **source_hashes,
            "unrelated-news-record": "c" * 64,
        }
    )
    feature = build_chase_risk_feature_vector(
        dataset,
        instrument=INSTRUMENT,
        valid_until=dataset.as_of + timedelta(hours=1),
        data_manifest=manifest,
    )
    context = DecisionContext(
        decision_time=dataset.as_of,
        decision_clock=DecisionClock.AFTER_CLOSE,
        knowledge_cutoff=dataset.as_of,
        trade_date=dataset.as_of.date(),
        universe_version="v4:universe:test:1",
        data_manifest=manifest,
        portfolio_policy_version="v4:portfolio:test:1",
        execution_contract_version="v4:execution:test:1",
        fee_schedule_version="v4:fees:test:1",
        account_snapshot_id="account-chase-test",
        code_commit_sha="a" * 40,
        config_hash="b" * 64,
        random_seed=7,
        capability_statuses={
            "daily_bar_chase_risk": CapabilityStatus(
                name="daily_bar_chase_risk",
                availability_status=AvailabilityStatus.ACTIVE,
                research_status=ResearchStatus.BACKTEST_READY,
                quality_status=QualityStatus.PASS,
            )
        },
    )
    account = AccountSnapshot(
        account_snapshot_id="account-chase-test",
        account_id="paper-chase-test",
        as_of=dataset.as_of,
        available_cash=Decimal("100000"),
        equity=Decimal("100000"),
    )
    rule = InstrumentRuleSnapshot(
        instrument=INSTRUMENT,
        rule_version="v4:rule:test:1",
        effective_at=dataset.as_of - timedelta(hours=1),
        knowledge_time=dataset.as_of,
        valid_until=dataset.as_of + timedelta(days=1),
        can_buy=True,
        can_sell=True,
        first_buy_minimum=100,
        buy_lot_size=100,
        sell_lot_size=1,
        settlement_days=1,
        tick_size=Decimal("0.01"),
        allow_odd_lot_liquidation=True,
    )

    decision_input = DecisionInput(
        context=context,
        account=account,
        scopes=(feature.scope,),
        feature_vectors=(feature,),
        instrument_rules=(rule,),
    )

    assert feature.source_manifest_hash == manifest.manifest_hash
    assert "unrelated-news-record" not in feature.source_record_hashes
    assert decision_input.feature_vectors == (feature,)


def test_chase_feature_rejects_manifest_with_a_forged_selected_hash() -> None:
    records = _base_and_limit_streak(4)
    dataset = _dataset(records)
    forged = {
        item.record_id: item.record_hash
        for item in records
    }
    forged[records[-1].record_id] = "f" * 64

    with pytest.raises(ValueError, match="absent from data_manifest"):
        build_chase_risk_feature_vector(
            dataset,
            instrument=INSTRUMENT,
            valid_until=dataset.as_of + timedelta(hours=1),
            data_manifest=DataManifest(forged),
        )
