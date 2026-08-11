from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import inspect

import pytest

import server.integrations.v2_execution_adapter.snapshot as adapter_module
from server.integrations.v2_execution_adapter import (
    map_v2_snapshot_match_inputs,
    match_v2_snapshot_read_only,
)
from server.trading_core.execution import (
    AttestedSnapshotQuote,
    MatchReason,
    MatchStatus,
    SnapshotEvidenceKind,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    match_attested_snapshot,
    snapshot_attestation_hash,
)
from server.trading_v2.domain import OrderSide, Quote
from server.trading_v2.matcher import PaperSnapshotMatcher
from server.trading_v2.policy import load_portfolio_policy


NOW = datetime(2026, 7, 27, 9, 31)
TICK = Decimal("0.01")
POLICY = load_portfolio_policy()


def _quote(**overrides: object) -> Quote:
    values: dict[str, object] = {
        "stock_code": "000001",
        "event_id": "snapshot-1",
        "quote_at": NOW,
        "received_at": NOW,
        "bid1": None,
        "bid1_volume": None,
        "ask1": None,
        "ask1_volume": None,
        "last_price": Decimal("10.00"),
        "upper_limit": Decimal("11.00"),
        "lower_limit": Decimal("9.00"),
        "suspended": False,
    }
    values.update(overrides)
    return Quote(**values)  # type: ignore[arg-type]


CASES = (
    pytest.param({}, {}, id="buy-full"),
    pytest.param(
        {"remaining_quantity": 1_000, "liquidity_quantity": 125},
        {},
        id="hard-liquidity-partial",
    ),
    pytest.param(
        {"remaining_quantity": 1_000, "approved_remaining_quantity": 175},
        {},
        id="approval-partial",
    ),
    pytest.param(
        {"side": OrderSide.SELL, "limit_price": Decimal("9.90")},
        {},
        id="sell-adverse-rounding",
    ),
    pytest.param({}, None, id="missing-snapshot"),
    pytest.param({}, {"last_price": None}, id="missing-last-price"),
    pytest.param(
        {},
        {
            "last_price": None,
            "quote_at": NOW - timedelta(seconds=181),
            "received_at": NOW,
        },
        id="missing-last-price-wins-over-stale",
    ),
    pytest.param(
        {},
        {"quote_at": NOW - timedelta(seconds=181), "received_at": NOW},
        id="stale-snapshot",
    ),
    pytest.param(
        {},
        {
            "quote_at": NOW + timedelta(seconds=1),
            "received_at": NOW + timedelta(seconds=1),
        },
        id="future-snapshot",
    ),
    pytest.param({}, {"suspended": True}, id="suspended"),
    pytest.param(
        {"limit_price": Decimal("11.00")},
        {"last_price": Decimal("11.00")},
        id="buy-upper-lock",
    ),
    pytest.param(
        {"side": OrderSide.SELL, "limit_price": Decimal("9.00")},
        {"last_price": Decimal("9.00")},
        id="sell-lower-lock",
    ),
    pytest.param(
        {"limit_price": Decimal("10.00")},
        {},
        id="buy-slippage-over-limit",
    ),
    pytest.param(
        {"side": OrderSide.SELL, "limit_price": Decimal("10.00")},
        {},
        id="sell-slippage-below-limit",
    ),
    pytest.param({}, {"upper_limit": None, "lower_limit": None}, id="no-band"),
    pytest.param(
        {"limit_price": Decimal("12.00")},
        {},
        id="legacy-limit-outside-band",
    ),
    pytest.param(
        {"remaining_quantity": 0},
        {},
        id="zero-remaining-compatibility",
    ),
    pytest.param(
        {"approved_remaining_quantity": -1},
        {},
        id="negative-approval-compatibility",
    ),
    pytest.param(
        {"liquidity_quantity": -1},
        {},
        id="negative-liquidity-compatibility",
    ),
)


@pytest.mark.parametrize(("call_overrides", "quote_overrides"), CASES)
def test_snapshot_adapter_is_differentially_equal_to_frozen_v2(
    call_overrides: dict[str, object],
    quote_overrides: dict[str, object] | None,
):
    call: dict[str, object] = {
        "side": OrderSide.BUY,
        "remaining_quantity": 100,
        "approved_remaining_quantity": 100,
        "limit_price": Decimal("10.10"),
        "quote": None if quote_overrides is None else _quote(**quote_overrides),
        "now": NOW,
        "tick_size": TICK,
        "liquidity_quantity": 100,
    }
    call.update(call_overrides)

    expected = PaperSnapshotMatcher(POLICY).match(**call)  # type: ignore[arg-type]
    actual = match_v2_snapshot_read_only(policy=POLICY, **call)

    assert actual.status == expected.status
    assert actual.waiting_reason == expected.waiting_reason
    assert actual.fill_quantity == expected.fill_quantity
    assert actual.fill_price == expected.fill_price
    assert actual.event_id == expected.event_id


def test_disabled_snapshot_fallback_is_differentially_equal_and_cannot_fill():
    disabled = replace(POLICY, paper_snapshot_fallback=False)
    call = {
        "side": OrderSide.BUY,
        "remaining_quantity": 100,
        "approved_remaining_quantity": 100,
        "limit_price": Decimal("10.10"),
        "quote": _quote(),
        "now": NOW,
        "tick_size": TICK,
        "liquidity_quantity": 100,
    }

    expected = PaperSnapshotMatcher(disabled).match(**call)
    actual = match_v2_snapshot_read_only(policy=disabled, **call)

    assert actual == replace(
        actual,
        status=expected.status,
        waiting_reason=expected.waiting_reason,
        fill_quantity=expected.fill_quantity,
        fill_price=expected.fill_price,
        event_id=expected.event_id,
    )
    assert actual.status == "WAITING"
    assert actual.event_id == ""


def test_attestation_source_and_payload_are_enforced_before_execution():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    assert mapped.quote is not None
    wrong_hash = replace(mapped.quote, attestation_hash="0" * 64)
    with pytest.raises(ValueError, match="attestation"):
        match_attested_snapshot(
            order=mapped.order,
            quote=wrong_hash,
            rule=mapped.rule,
            evaluated_at=mapped.evaluated_at,
        )

    disallowed_source = "untrusted-source"
    mismatched_provider = replace(
        mapped.quote,
        source=disallowed_source,
        attestation_hash=snapshot_attestation_hash(
            instrument_id=mapped.quote.instrument_id,
            snapshot_id=mapped.quote.snapshot_id,
            observed_at=mapped.quote.observed_at,
            received_at=mapped.quote.received_at,
            last_price=mapped.quote.last_price,
            source=disallowed_source,
            liquidity_evidence_hash=(
                mapped.quote.liquidity_evidence.evidence_hash
            ),
            suspended=mapped.quote.suspended,
        ),
    )
    with pytest.raises(ValueError, match="provider does not match"):
        match_attested_snapshot(
            order=mapped.order,
            quote=mismatched_provider,
            rule=mapped.rule,
            evaluated_at=mapped.evaluated_at,
        )

    disallowed_evidence = replace(
        mapped.quote.liquidity_evidence,
        source_provider=disallowed_source,
    )
    disallowed = replace(
        mapped.quote,
        source=disallowed_source,
        liquidity_evidence=disallowed_evidence,
        attestation_hash=snapshot_attestation_hash(
            instrument_id=mapped.quote.instrument_id,
            snapshot_id=mapped.quote.snapshot_id,
            observed_at=mapped.quote.observed_at,
            received_at=mapped.quote.received_at,
            last_price=mapped.quote.last_price,
            source=disallowed_source,
            liquidity_evidence_hash=disallowed_evidence.evidence_hash,
            suspended=mapped.quote.suspended,
        ),
    )
    decision = match_attested_snapshot(
        order=mapped.order,
        quote=disallowed,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )
    assert decision.status == MatchStatus.WAITING
    assert decision.reason == MatchReason.WAIT_NO_QUOTE

    production_default = replace(
        mapped.rule,
        allow_synthetic_compatibility_evidence=False,
    )
    synthetic_blocked = match_attested_snapshot(
        order=mapped.order,
        quote=mapped.quote,
        rule=production_default,
        evaluated_at=mapped.evaluated_at,
    )
    assert synthetic_blocked.status == MatchStatus.WAITING
    assert synthetic_blocked.reason == MatchReason.WAIT_NO_QUOTE


def test_synthetic_compatibility_evidence_does_not_invent_authority_facts():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=37,
        policy=POLICY,
    )
    assert mapped.quote is not None
    evidence = mapped.quote.liquidity_evidence

    assert (
        evidence.evidence_kind
        == SnapshotEvidenceKind.SYNTHETIC_STANDALONE_COMPATIBILITY
    )
    assert evidence.source_receipt_hash is None
    assert evidence.quality_status == "NOT_ASSESSED"
    assert evidence.source_volume is None
    assert evidence.lot_size is None
    assert evidence.participation_rate is None
    assert evidence.already_filled_quantity is None
    assert evidence.standalone_compatibility_quantity == 37
    assert evidence.liquidity_quantity == 37
    assert "CANONICAL_V2_RECEIPT" not in SnapshotEvidenceKind.__members__

    with pytest.raises(ValueError, match="must not invent a receipt"):
        replace(evidence, source_receipt_hash="0" * 64)


def test_external_receipt_reference_derives_but_does_not_authenticate_cap():
    evidence = SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE,
        source_provider="verified-upstream-boundary",
        source_batch_id="batch-1",
        source_payload_hash="a" * 64,
        source_receipt_hash="b" * 64,
        quality_status="PASS",
        source_count=2,
        source_volume=1_000,
        lot_size=100,
        participation_rate=Decimal("0.25"),
        already_filled_quantity=100,
    )

    # 1,000 * 25% is floored to two 100-share lots, then the prior 100-share
    # fill is removed. Hashes bind these inputs but do not verify the issuer.
    assert evidence.liquidity_quantity == 100
    assert len(evidence.evidence_hash) == 64


def test_snapshot_retry_is_idempotent_and_conflicting_event_is_rejected():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=200,
        approved_remaining_quantity=200,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    first = match_attested_snapshot(
        order=mapped.order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )
    retry = match_attested_snapshot(
        order=first.updated_order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )

    assert first.status == MatchStatus.PARTIALLY_FILLED
    assert retry.status == MatchStatus.DUPLICATE
    assert retry.updated_order is first.updated_order
    assert mapped.quote is not None
    conflicting = replace(mapped.quote, attestation_hash="f" * 64)
    with pytest.raises(ValueError, match="different semantics"):
        match_attested_snapshot(
            order=first.updated_order,
            quote=conflicting,
            rule=mapped.rule,
            evaluated_at=mapped.evaluated_at,
        )


def test_snapshot_matcher_rejects_subclass_contract_bypass():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )

    class ForgedQuote(AttestedSnapshotQuote):
        pass

    class ForgedRule(SnapshotMatchRule):
        pass

    with pytest.raises(TypeError, match="exactly AttestedSnapshotQuote"):
        match_attested_snapshot(
            order=mapped.order,
            quote=object.__new__(ForgedQuote),
            rule=mapped.rule,
            evaluated_at=mapped.evaluated_at,
        )
    with pytest.raises(TypeError, match="exactly SnapshotMatchRule"):
        match_attested_snapshot(
            order=mapped.order,
            quote=mapped.quote,
            rule=object.__new__(ForgedRule),
            evaluated_at=mapped.evaluated_at,
        )

    class EvilDecimal(Decimal):
        def __mul__(self, other: object):
            return Decimal("1")

    with pytest.raises(TypeError, match="Decimal subclass"):
        replace(mapped.quote, last_price=EvilDecimal("100"))
    with pytest.raises(TypeError, match="Decimal subclass"):
        replace(mapped.rule, slippage_rate=EvilDecimal("0.01"))

    external = SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE,
        source_provider="verified-upstream-boundary",
        source_batch_id="batch-1",
        source_payload_hash="a" * 64,
        source_receipt_hash="b" * 64,
        quality_status="PASS",
        source_count=1,
        source_volume=1_000,
        lot_size=100,
        participation_rate=Decimal("0.10"),
        already_filled_quantity=0,
    )
    with pytest.raises(TypeError, match="Decimal subclass"):
        replace(external, participation_rate=EvilDecimal("0.10"))

    class EvilDatetime(datetime):
        pass

    with pytest.raises(TypeError, match="exactly datetime"):
        replace(
            mapped.quote,
            observed_at=EvilDatetime(2026, 7, 27, 9, 31),
        )


def test_waiting_snapshot_event_id_cannot_be_reused_with_changed_payload():
    stale = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(
            quote_at=NOW - timedelta(seconds=181),
            received_at=NOW,
        ),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    first = match_attested_snapshot(
        order=stale.order,
        quote=stale.quote,
        rule=stale.rule,
        evaluated_at=stale.evaluated_at,
    )
    retry = match_attested_snapshot(
        order=first.updated_order,
        quote=stale.quote,
        rule=stale.rule,
        evaluated_at=stale.evaluated_at,
    )

    assert first.status == MatchStatus.WAITING
    assert retry.status == MatchStatus.DUPLICATE
    assert retry.reason == MatchReason.DUPLICATE_EVENT
    assert retry.updated_order is first.updated_order
    assert stale.quote is not None
    changed_price = Decimal("9.50")
    changed = replace(
        stale.quote,
        last_price=changed_price,
        attestation_hash=snapshot_attestation_hash(
            instrument_id=stale.quote.instrument_id,
            snapshot_id=stale.quote.snapshot_id,
            observed_at=stale.quote.observed_at,
            received_at=stale.quote.received_at,
            last_price=changed_price,
            source=stale.quote.source,
            liquidity_evidence_hash=stale.quote.liquidity_evidence.evidence_hash,
            suspended=stale.quote.suspended,
        ),
    )
    with pytest.raises(ValueError, match="different semantics"):
        match_attested_snapshot(
            order=first.updated_order,
            quote=changed,
            rule=stale.rule,
            evaluated_at=stale.evaluated_at,
        )


def test_snapshot_matcher_revalidates_low_level_evidence_mutation():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    assert mapped.quote is not None
    forged_evidence = replace(mapped.quote.liquidity_evidence)
    forged_quote = replace(
        mapped.quote,
        liquidity_evidence=forged_evidence,
    )
    object.__setattr__(forged_evidence, "liquidity_quantity", 1_000_000)

    with pytest.raises(ValueError, match="cannot be reconstructed"):
        match_attested_snapshot(
            order=mapped.order,
            quote=forged_quote,
            rule=mapped.rule,
            evaluated_at=mapped.evaluated_at,
        )


def test_not_active_waiting_snapshot_retry_is_idempotent():
    mapped = map_v2_snapshot_match_inputs(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=_quote(),
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    waiting_order = replace(
        mapped.order,
        earliest_at=mapped.evaluated_at + timedelta(minutes=1),
        expires_at=mapped.evaluated_at + timedelta(days=1),
        updated_at=mapped.evaluated_at,
    )
    first = match_attested_snapshot(
        order=waiting_order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )
    retry = match_attested_snapshot(
        order=first.updated_order,
        quote=mapped.quote,
        rule=mapped.rule,
        evaluated_at=mapped.evaluated_at,
    )

    assert first.status == MatchStatus.WAITING
    assert first.reason == MatchReason.WAIT_NOT_ACTIVE
    assert retry.status == MatchStatus.DUPLICATE
    assert retry.updated_order is first.updated_order


def test_future_received_at_is_a_deliberate_fail_closed_v2_delta():
    future_receipt = _quote(received_at=NOW + timedelta(seconds=1))
    legacy_fill = PaperSnapshotMatcher(POLICY).match(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=future_receipt,
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
    )
    neutral_wait = match_v2_snapshot_read_only(
        side=OrderSide.BUY,
        remaining_quantity=100,
        approved_remaining_quantity=100,
        limit_price=Decimal("10.10"),
        quote=future_receipt,
        now=NOW,
        tick_size=TICK,
        liquidity_quantity=100,
        policy=POLICY,
    )
    assert legacy_fill.status == "FILLED"
    assert neutral_wait.status == "WAITING"
    assert neutral_wait.waiting_reason == "WAIT_STALE_QUOTE"


def test_snapshot_adapter_has_no_write_capable_imports():
    tree = ast.parse(inspect.getsource(adapter_module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    forbidden = (
        "server.trading_v2.execution",
        "server.trading_v2.repository",
        "server.trading_v2.ledger",
        "sqlalchemy",
    )
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in imported
        for prefix in forbidden
    )
