from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import inspect
from itertools import permutations

import pytest

import server.trading_core.execution.snapshot_batch as batch_module
from server.trading_core.contracts import OrderSide, OrderStatus
from server.trading_core.execution import (
    AttestedSnapshotQuote,
    LimitDayOrder,
    MatchReason,
    MatchStatus,
    SnapshotBatchCandidate,
    SnapshotBatchRequest,
    SnapshotEvidenceKind,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    match_snapshot_batch,
    snapshot_attestation_hash,
    validate_snapshot_batch_result,
)


MARKET_TZ = timezone(timedelta(hours=8))
NOW = datetime(2026, 7, 27, 9, 31, tzinfo=MARKET_TZ)
SOURCE = "verified-snapshot-feed"
PAYLOAD_HASH = "a" * 64
RECEIPT_HASH = "b" * 64


def _evidence(
    *,
    source: str = SOURCE,
    receipt_hash: str = RECEIPT_HASH,
    source_volume: int = 1_000,
    lot_size: int = 1,
    participation_rate: Decimal = Decimal("0.10"),
    already_filled_quantity: int = 0,
) -> SnapshotLiquidityEvidence:
    return SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE,
        source_provider=source,
        source_batch_id="batch-1",
        source_payload_hash=PAYLOAD_HASH,
        source_receipt_hash=receipt_hash,
        quality_status="PASS",
        source_count=2,
        source_volume=source_volume,
        lot_size=lot_size,
        participation_rate=participation_rate,
        already_filled_quantity=already_filled_quantity,
    )


def _quote(
    *,
    instrument_id: str = "000001",
    snapshot_id: str = "snapshot-1",
    source: str = SOURCE,
    observed_at: datetime | None = None,
    received_at: datetime | None = None,
    evidence: SnapshotLiquidityEvidence | None = None,
    last_price: Decimal | None = Decimal("10.00"),
) -> AttestedSnapshotQuote:
    observed = observed_at or NOW - timedelta(seconds=1)
    received = received_at or NOW
    liquidity_evidence = evidence or _evidence(source=source)
    attestation = snapshot_attestation_hash(
        instrument_id=instrument_id,
        snapshot_id=snapshot_id,
        observed_at=observed,
        received_at=received,
        last_price=last_price,
        source=source,
        liquidity_evidence_hash=liquidity_evidence.evidence_hash,
        suspended=False,
    )
    return AttestedSnapshotQuote(
        instrument_id=instrument_id,
        snapshot_id=snapshot_id,
        observed_at=observed,
        received_at=received,
        last_price=last_price,
        source=source,
        attestation_hash=attestation,
        liquidity_evidence=liquidity_evidence,
        suspended=False,
    )


def _rule(
    *,
    version: str = "snapshot-batch-rule-v1",
    allowed_sources: tuple[str, ...] = (SOURCE,),
    allow_synthetic: bool = False,
) -> SnapshotMatchRule:
    return SnapshotMatchRule(
        rule_version=version,
        enabled=True,
        tick_size=Decimal("0.01"),
        quote_max_age=timedelta(seconds=60),
        allowed_sources=allowed_sources,
        allow_synthetic_compatibility_evidence=allow_synthetic,
        slippage_rate=Decimal("0"),
        require_complete_price_band=False,
        enforce_price_band_bounds=False,
        block_adverse_limit_lock=False,
    )


def _order(
    order_id: str,
    *,
    instrument_id: str = "000001",
    quantity: int = 80,
    approved_quantity: int | None = None,
    earliest_at: datetime | None = None,
    limit_price: Decimal = Decimal("11.00"),
) -> LimitDayOrder:
    earliest = earliest_at or NOW - timedelta(minutes=1)
    return LimitDayOrder(
        order_id=order_id,
        intent_id=f"intent-{order_id}",
        instrument_id=instrument_id,
        side=OrderSide.BUY,
        requested_quantity=quantity,
        approved_quantity=(
            quantity if approved_quantity is None else approved_quantity
        ),
        cumulative_filled_quantity=0,
        limit_price=limit_price,
        earliest_at=earliest,
        expires_at=NOW + timedelta(hours=4),
        updated_at=earliest,
        status=OrderStatus.QUEUED,
    )


def _candidate(
    order_id: str,
    *,
    quantity: int = 80,
    approved_quantity: int | None = None,
    created_at: datetime | None = None,
    earliest_at: datetime | None = None,
    limit_price: Decimal = Decimal("11.00"),
    instrument_id: str = "000001",
    quote: AttestedSnapshotQuote | None = None,
    rule: SnapshotMatchRule | None = None,
    evaluated_at: datetime = NOW,
) -> SnapshotBatchCandidate:
    return SnapshotBatchCandidate(
        created_at=created_at or NOW - timedelta(minutes=10),
        evaluated_at=evaluated_at,
        order=_order(
            order_id,
            instrument_id=instrument_id,
            quantity=quantity,
            approved_quantity=approved_quantity,
            earliest_at=earliest_at,
            limit_price=limit_price,
        ),
        quote=quote or _quote(instrument_id=instrument_id),
        rule=rule or _rule(),
    )


def test_batch_allocates_one_shared_cap_by_mechanical_priority():
    earlier = _candidate(
        "order-z",
        created_at=NOW - timedelta(minutes=11),
    )
    later = _candidate(
        "order-a",
        created_at=NOW - timedelta(minutes=10),
    )
    request = SnapshotBatchRequest(candidates=(later, earlier))
    result = match_snapshot_batch(request=request)

    assert tuple(item.order_id for item in result.allocations) == (
        "order-z",
        "order-a",
    )
    assert result.shared_liquidity_cap == 100
    assert result.total_fill_quantity == 100
    assert tuple(
        item.decision.fill_quantity for item in result.allocations
    ) == (80, 20)
    assert result.allocations[0].decision.status == MatchStatus.FILLED
    assert result.allocations[1].decision.status == MatchStatus.PARTIALLY_FILLED
    assert (
        result.allocations[0].shared_cap_before,
        result.allocations[0].shared_cap_after,
        result.allocations[1].shared_cap_before,
        result.allocations[1].shared_cap_after,
    ) == (100, 20, 20, 0)
    assert sum(
        item.decision.fill_quantity for item in result.allocations
    ) <= result.shared_liquidity_cap
    for allocation in result.allocations:
        execution = allocation.decision.execution_result
        assert execution is not None
        assert execution.order_id == allocation.order_id
        assert execution.event_id == "snapshot-1"
        assert execution.last_fill_quantity == allocation.decision.fill_quantity
        assert execution.last_fill_price == allocation.decision.fill_price
    validate_snapshot_batch_result(result)


def test_participation_lot_cap_minus_already_filled_is_shared_once():
    evidence = _evidence(
        source_volume=1_000,
        lot_size=100,
        participation_rate=Decimal("0.25"),
        already_filled_quantity=50,
    )
    quote = _quote(evidence=evidence)
    first = _candidate(
        "order-1",
        quantity=100,
        created_at=NOW - timedelta(minutes=12),
        quote=quote,
    )
    second = _candidate(
        "order-2",
        quantity=100,
        created_at=NOW - timedelta(minutes=11),
        quote=quote,
    )

    result = match_snapshot_batch(
        request=SnapshotBatchRequest(candidates=(first, second))
    )

    # floor(1,000 * 25%, 100-share lots) - 50 already filled = 150.
    assert result.shared_liquidity_cap == 150
    assert result.total_fill_quantity == 150
    assert tuple(
        item.decision.fill_quantity for item in result.allocations
    ) == (100, 50)


@pytest.mark.parametrize(
    ("evidence", "first_approved", "expected"),
    (
        pytest.param(
            _evidence(
                source_volume=0,
                participation_rate=Decimal("1"),
            ),
            None,
            (0, 0),
            id="zero-cap",
        ),
        pytest.param(
            _evidence(
                source_volume=1,
                participation_rate=Decimal("1"),
            ),
            None,
            (1, 0),
            id="one-unit-cap",
        ),
        pytest.param(
            _evidence(
                source_volume=10,
                participation_rate=Decimal("1"),
            ),
            3,
            (3, 7),
            id="approval-smaller-than-residual",
        ),
    ),
)
def test_shared_cap_boundaries(
    evidence: SnapshotLiquidityEvidence,
    first_approved: int | None,
    expected: tuple[int, int],
):
    quote = _quote(evidence=evidence)
    first = _candidate(
        "order-1",
        quantity=10,
        approved_quantity=first_approved,
        created_at=NOW - timedelta(minutes=12),
        quote=quote,
    )
    second = _candidate(
        "order-2",
        quantity=10,
        created_at=NOW - timedelta(minutes=11),
        quote=quote,
    )

    result = match_snapshot_batch(
        request=SnapshotBatchRequest(candidates=(first, second))
    )

    assert tuple(
        allocation.decision.fill_quantity for allocation in result.allocations
    ) == expected
    assert result.total_fill_quantity == sum(expected)
    assert result.total_fill_quantity <= result.shared_liquidity_cap


def test_waiting_order_does_not_consume_cap_before_next_order():
    blocked = _candidate(
        "order-blocked",
        created_at=NOW - timedelta(minutes=12),
        limit_price=Decimal("9.00"),
        quantity=100,
    )
    executable = _candidate(
        "order-executable",
        created_at=NOW - timedelta(minutes=11),
        quantity=100,
    )

    result = match_snapshot_batch(
        request=SnapshotBatchRequest(candidates=(executable, blocked))
    )

    first, second = result.allocations
    assert first.order_id == "order-blocked"
    assert first.decision.status == MatchStatus.WAITING
    assert first.decision.reason == MatchReason.WAIT_LIQUIDITY
    assert first.shared_cap_before == first.shared_cap_after == 100
    assert second.decision.fill_quantity == 100
    assert second.shared_cap_before == 100
    assert second.shared_cap_after == 0


def test_priority_and_hashes_are_independent_of_input_permutation():
    same_created = NOW - timedelta(minutes=10)
    earlier_window = _candidate(
        "order-z",
        quantity=40,
        created_at=same_created,
        earliest_at=NOW - timedelta(minutes=2),
    )
    later_window_low_id = _candidate(
        "order-0",
        quantity=40,
        created_at=same_created,
        earliest_at=NOW - timedelta(minutes=1),
    )
    later_window_high_id = _candidate(
        "order-a",
        quantity=40,
        created_at=same_created,
        earliest_at=NOW - timedelta(minutes=1),
    )
    expected = None
    for ordering in permutations(
        (earlier_window, later_window_low_id, later_window_high_id)
    ):
        request = SnapshotBatchRequest(candidates=ordering)
        result = match_snapshot_batch(request=request)
        current = (
            request.request_hash,
            result.result_hash,
            tuple(item.order_id for item in result.allocations),
            tuple(item.decision.fill_quantity for item in result.allocations),
        )
        if expected is None:
            expected = current
        assert current == expected
    assert expected is not None
    assert expected[2] == ("order-z", "order-0", "order-a")
    assert expected[3] == (40, 40, 20)

    candidate_fields = {field.name for field in fields(SnapshotBatchCandidate)}
    assert candidate_fields == {
        "created_at",
        "evaluated_at",
        "order",
        "quote",
        "rule",
        "candidate_hash",
    }


def test_equal_instants_in_different_timezones_fall_through_to_order_id():
    created_market = NOW - timedelta(minutes=10)
    created_utc = created_market.astimezone(timezone.utc)
    high_id = _candidate("order-z", created_at=created_market)
    low_id = _candidate("order-a", created_at=created_utc)

    request = SnapshotBatchRequest(candidates=(high_id, low_id))

    assert tuple(item.order.order_id for item in request.candidates) == (
        "order-a",
        "order-z",
    )


def test_duplicate_and_inconsistent_batch_facts_fail_closed():
    first = _candidate("order-1")
    with pytest.raises(ValueError, match="order_id values must be unique"):
        SnapshotBatchRequest(candidates=(first, first))

    other_instrument = _candidate(
        "order-2",
        instrument_id="000002",
        quote=_quote(instrument_id="000002"),
    )
    with pytest.raises(ValueError, match="share one instrument"):
        SnapshotBatchRequest(candidates=(first, other_instrument))

    other_snapshot = _candidate(
        "order-2",
        quote=_quote(snapshot_id="snapshot-2"),
    )
    with pytest.raises(ValueError, match="share one snapshot_id"):
        SnapshotBatchRequest(candidates=(first, other_snapshot))

    other_receipt_evidence = _evidence(receipt_hash="c" * 64)
    other_receipt = _candidate(
        "order-2",
        quote=_quote(evidence=other_receipt_evidence),
    )
    with pytest.raises(ValueError, match="share one external receipt"):
        SnapshotBatchRequest(candidates=(first, other_receipt))

    shared_rule = _rule(allowed_sources=(SOURCE, "other-feed"))
    source_first = _candidate("order-1", rule=shared_rule)
    source_second = _candidate(
        "order-2",
        quote=_quote(
            source="other-feed",
            evidence=_evidence(source="other-feed"),
        ),
        rule=shared_rule,
    )
    with pytest.raises(ValueError, match="sources are inconsistent"):
        SnapshotBatchRequest(candidates=(source_first, source_second))

    other_time = _candidate(
        "order-2",
        quote=_quote(
            observed_at=NOW - timedelta(seconds=2),
            received_at=NOW,
        ),
    )
    with pytest.raises(ValueError, match="times are inconsistent"):
        SnapshotBatchRequest(candidates=(first, other_time))

    other_evaluation = _candidate(
        "order-2",
        evaluated_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="times are inconsistent"):
        SnapshotBatchRequest(candidates=(first, other_evaluation))

    other_rule = _candidate(
        "order-2",
        rule=_rule(version="snapshot-batch-rule-v2"),
    )
    with pytest.raises(ValueError, match="rules are inconsistent"):
        SnapshotBatchRequest(candidates=(first, other_rule))


def test_synthetic_or_synthetic_enabled_rule_is_never_accepted():
    synthetic = SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.SYNTHETIC_STANDALONE_COMPATIBILITY,
        source_provider=SOURCE,
        source_batch_id="compat:snapshot-1",
        source_payload_hash=PAYLOAD_HASH,
        source_receipt_hash=None,
        quality_status="NOT_ASSESSED",
        source_count=1,
        standalone_compatibility_quantity=100,
    )
    synthetic_quote = _quote(evidence=synthetic)
    with pytest.raises(ValueError, match="only EXTERNAL_RECEIPT_REFERENCE"):
        _candidate("order-1", quote=synthetic_quote)

    with pytest.raises(ValueError, match="cannot allow synthetic"):
        _candidate("order-1", rule=_rule(allow_synthetic=True))


def test_exact_frozen_contracts_and_subclass_bypasses_are_rejected():
    candidate = _candidate("order-1")
    with pytest.raises(FrozenInstanceError):
        candidate.created_at = NOW  # type: ignore[misc]
    with pytest.raises(TypeError, match="candidates must be exactly tuple"):
        SnapshotBatchRequest(candidates=[candidate])  # type: ignore[arg-type]

    class ForgedCandidate(SnapshotBatchCandidate):
        pass

    with pytest.raises(TypeError, match="exactly SnapshotBatchCandidate"):
        SnapshotBatchRequest(candidates=(object.__new__(ForgedCandidate),))

    class EvilDatetime(datetime):
        pass

    with pytest.raises(TypeError, match="exactly datetime"):
        SnapshotBatchCandidate(
            created_at=EvilDatetime(2026, 7, 27, 9, 0, tzinfo=MARKET_TZ),
            evaluated_at=NOW,
            order=candidate.order,
            quote=candidate.quote,
            rule=candidate.rule,
        )

    class EvilDecimal(Decimal):
        pass

    with pytest.raises(TypeError, match="Decimal subclass"):
        _evidence(participation_rate=EvilDecimal("0.10"))


def test_low_level_derived_hash_and_quantity_tampering_is_detected():
    evidence = _evidence()
    quote = _quote(evidence=evidence)
    object.__setattr__(evidence, "liquidity_quantity", 10_000)
    with pytest.raises(ValueError, match="derived fields or hash were tampered"):
        _candidate("order-1", quote=quote)

    evidence = _evidence()
    quote = _quote(evidence=evidence)
    object.__setattr__(evidence, "evidence_hash", "f" * 64)
    with pytest.raises(ValueError, match="derived fields or hash were tampered"):
        _candidate("order-1", quote=quote)

    quote = _quote()
    object.__setattr__(quote, "attestation_hash", "f" * 64)
    with pytest.raises(ValueError, match="attestation hash was tampered"):
        _candidate("order-1", quote=quote)

    candidate = _candidate("order-1")
    object.__setattr__(candidate, "candidate_hash", "f" * 64)
    with pytest.raises(ValueError, match="candidate fields or hash were tampered"):
        SnapshotBatchRequest(candidates=(candidate,))

    request = SnapshotBatchRequest(candidates=(_candidate("order-1"),))
    object.__setattr__(request, "request_hash", "f" * 64)
    with pytest.raises(ValueError, match="request fields or hash were tampered"):
        match_snapshot_batch(request=request)

    result = match_snapshot_batch(
        request=SnapshotBatchRequest(candidates=(_candidate("order-1"),))
    )
    object.__setattr__(result, "result_hash", "f" * 64)
    with pytest.raises(ValueError, match="result fields or hash were tampered"):
        validate_snapshot_batch_result(result)


def test_replayed_or_mixed_old_and_new_snapshot_orders_fail_closed():
    first = _candidate(
        "order-1",
        quantity=200,
        created_at=NOW - timedelta(minutes=12),
    )
    second = _candidate(
        "order-2",
        quantity=200,
        created_at=NOW - timedelta(minutes=11),
    )
    original = SnapshotBatchRequest(candidates=(first, second))
    result = match_snapshot_batch(request=original)
    updated_first = SnapshotBatchCandidate(
        created_at=first.created_at,
        evaluated_at=first.evaluated_at,
        order=result.allocations[0].decision.updated_order,
        quote=first.quote,
        rule=first.rule,
    )
    fresh = _candidate("order-3")

    with pytest.raises(ValueError, match="one-shot"):
        SnapshotBatchRequest(candidates=(updated_first, fresh))


def test_snapshot_batch_module_has_no_write_or_strategy_imports():
    tree = ast.parse(inspect.getsource(batch_module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    forbidden_fragments = (
        "sqlalchemy",
        "repository",
        "ledger",
        "account",
        "broker",
        "strategy",
        "server.integrations",
    )
    assert not any(
        fragment in module
        for module in imported
        for fragment in forbidden_fragments
    )
