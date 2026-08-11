from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import inspect
from zoneinfo import ZoneInfo

import pytest

import server.trading_core.execution.session_gate as gate_module
from server.trading_core.contracts import OrderSide, OrderStatus
from server.trading_core.execution import (
    AttestedSnapshotQuote,
    Level1Quote,
    LimitDayMatchRule,
    LimitDayOrder,
    LocalTradingSession,
    MatchStatus,
    SessionCalendarEvidenceKind,
    SessionGateReason,
    SessionGatedMatchDecision,
    SessionGatedSnapshotBatchDecision,
    SnapshotBatchCandidate,
    SnapshotBatchRequest,
    SnapshotEvidenceKind,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    TradingSessionCalendarEvidence,
    match_attested_snapshot_in_session,
    match_limit_day_in_session,
    match_snapshot_batch_in_session,
    snapshot_attestation_hash,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 3)
AVAILABLE_AT = datetime(2026, 8, 2, 16, 0, tzinfo=SHANGHAI)
FIRST_OPEN = datetime(2026, 8, 3, 9, 30, tzinfo=SHANGHAI)
FINAL_CLOSE = datetime(2026, 8, 3, 15, 0, tzinfo=SHANGHAI)
SOURCE = "verified-snapshot-feed"


def _calendar(
    *,
    evidence_kind: SessionCalendarEvidenceKind = (
        SessionCalendarEvidenceKind.EXTERNAL_RECEIPT_REFERENCE
    ),
    source_receipt_hash: str | None = "b" * 64,
    quality_status: str = "PASS",
    available_at: datetime = AVAILABLE_AT,
) -> TradingSessionCalendarEvidence:
    return TradingSessionCalendarEvidence(
        evidence_kind=evidence_kind,
        calendar_version="calendar-20260803",
        market_timezone="Asia/Shanghai",
        trade_date=TRADE_DATE,
        trading_days=(date(2026, 7, 31), TRADE_DATE),
        sessions=(
            LocalTradingSession("MORNING", time(9, 30), time(11, 30)),
            LocalTradingSession("AFTERNOON", time(13, 0), time(15, 0)),
        ),
        available_at=available_at,
        source_provider="verified-calendar-boundary",
        source_payload_hash="a" * 64,
        source_receipt_hash=source_receipt_hash,
        quality_status=quality_status,
    )


def _order(
    order_id: str = "order-1",
    *,
    quantity: int = 100,
    earliest_at: datetime = FIRST_OPEN,
    expires_at: datetime = FINAL_CLOSE,
    updated_at: datetime = datetime(
        2026,
        8,
        2,
        15,
        0,
        tzinfo=SHANGHAI,
    ),
    limit_price: Decimal = Decimal("11.00"),
) -> LimitDayOrder:
    return LimitDayOrder(
        order_id=order_id,
        intent_id=f"intent-{order_id}",
        instrument_id="000001",
        side=OrderSide.BUY,
        requested_quantity=quantity,
        approved_quantity=quantity,
        cumulative_filled_quantity=0,
        limit_price=limit_price,
        earliest_at=earliest_at,
        expires_at=expires_at,
        updated_at=updated_at,
        status=OrderStatus.QUEUED,
    )


def _level1_quote(
    at: datetime,
    *,
    quote_id: str = "quote-1",
    ask_price: Decimal = Decimal("10.00"),
) -> Level1Quote:
    return Level1Quote(
        instrument_id="000001",
        quote_id=quote_id,
        observed_at=at,
        received_at=at,
        bid_price=Decimal("9.99"),
        bid_quantity=1_000,
        ask_price=ask_price,
        ask_quantity=1_000,
        suspended=False,
    )


def _level1_rule() -> LimitDayMatchRule:
    return LimitDayMatchRule(
        rule_version="level1-session-rule-v1",
        tick_size=Decimal("0.01"),
        quote_max_age=timedelta(days=1),
        visible_volume_participation=Decimal("1"),
        require_complete_price_band=False,
        enforce_price_band_bounds=False,
        block_adverse_limit_lock=False,
    )


def _snapshot_evidence() -> SnapshotLiquidityEvidence:
    return SnapshotLiquidityEvidence(
        evidence_kind=SnapshotEvidenceKind.EXTERNAL_RECEIPT_REFERENCE,
        source_provider=SOURCE,
        source_batch_id="snapshot-batch-1",
        source_payload_hash="c" * 64,
        source_receipt_hash="d" * 64,
        quality_status="PASS",
        source_count=2,
        source_volume=1_000,
        lot_size=1,
        participation_rate=Decimal("0.10"),
        already_filled_quantity=0,
    )


def _snapshot_quote(
    at: datetime,
    *,
    snapshot_id: str = "snapshot-1",
    last_price: Decimal = Decimal("10.00"),
    evidence: SnapshotLiquidityEvidence | None = None,
) -> AttestedSnapshotQuote:
    liquidity_evidence = evidence or _snapshot_evidence()
    attestation = snapshot_attestation_hash(
        instrument_id="000001",
        snapshot_id=snapshot_id,
        observed_at=at,
        received_at=at,
        last_price=last_price,
        source=SOURCE,
        liquidity_evidence_hash=liquidity_evidence.evidence_hash,
        suspended=False,
    )
    return AttestedSnapshotQuote(
        instrument_id="000001",
        snapshot_id=snapshot_id,
        observed_at=at,
        received_at=at,
        last_price=last_price,
        source=SOURCE,
        attestation_hash=attestation,
        liquidity_evidence=liquidity_evidence,
        suspended=False,
    )


def _snapshot_rule() -> SnapshotMatchRule:
    return SnapshotMatchRule(
        rule_version="snapshot-session-rule-v1",
        enabled=True,
        tick_size=Decimal("0.01"),
        quote_max_age=timedelta(days=1),
        allowed_sources=(SOURCE,),
        allow_synthetic_compatibility_evidence=False,
        require_complete_price_band=False,
        enforce_price_band_bounds=False,
        block_adverse_limit_lock=False,
    )


def _batch_request(
    evaluated_at: datetime,
    *,
    quote: AttestedSnapshotQuote | None = None,
    orders: tuple[LimitDayOrder, ...] | None = None,
) -> SnapshotBatchRequest:
    shared_quote = quote or _snapshot_quote(evaluated_at - timedelta(seconds=1))
    batch_orders = orders or (
        _order("order-1", quantity=80),
        _order("order-2", quantity=80),
    )
    candidates = tuple(
        SnapshotBatchCandidate(
            created_at=AVAILABLE_AT,
            evaluated_at=evaluated_at,
            order=order,
            quote=shared_quote,
            rule=_snapshot_rule(),
        )
        for order in batch_orders
    )
    return SnapshotBatchRequest(candidates=candidates)


@pytest.mark.parametrize(
    ("evaluated_at", "expected_reason"),
    (
        (
            datetime(2026, 8, 2, 15, 59, tzinfo=SHANGHAI),
            SessionGateReason.SESSION_NOT_OBSERVABLE,
        ),
        (
            datetime(2026, 8, 3, 9, 20, tzinfo=SHANGHAI),
            SessionGateReason.SESSION_PRE_OPEN,
        ),
        (
            datetime(2026, 8, 3, 11, 45, tzinfo=SHANGHAI),
            SessionGateReason.SESSION_BREAK,
        ),
    ),
)
def test_inactive_sessions_gate_level1_and_snapshot_with_mechanical_reason(
    evaluated_at: datetime,
    expected_reason: SessionGateReason,
):
    level1 = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(evaluated_at - timedelta(seconds=1)),
        rule=_level1_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )
    snapshot = match_attested_snapshot_in_session(
        order=_order(),
        quote=_snapshot_quote(evaluated_at - timedelta(seconds=1)),
        rule=_snapshot_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )

    for result in (level1, snapshot):
        assert result.gate_reason is expected_reason
        assert result.decision.status is MatchStatus.WAITING
        assert result.decision.fill_quantity == 0
        assert any(
            event_id.startswith("session-gate:")
            for event_id, _ in result.decision.updated_order.applied_events
        )
        assert len(result.decision_hash) == 64


def test_live_inactive_orders_do_not_delegate_to_matchers(monkeypatch):
    evaluated_at = datetime(2026, 8, 3, 9, 20, tzinfo=SHANGHAI)

    def unexpected(**_: object):
        raise AssertionError("inactive live order delegated to matcher")

    monkeypatch.setattr(gate_module, "match_limit_day", unexpected)
    monkeypatch.setattr(gate_module, "match_attested_snapshot", unexpected)
    monkeypatch.setattr(gate_module, "match_snapshot_batch", unexpected)

    assert match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(evaluated_at),
        rule=_level1_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    ).decision.status is MatchStatus.WAITING
    assert match_attested_snapshot_in_session(
        order=_order(),
        quote=_snapshot_quote(evaluated_at),
        rule=_snapshot_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    ).decision.status is MatchStatus.WAITING
    assert match_snapshot_batch_in_session(
        request=_batch_request(evaluated_at),
        calendar_evidence=_calendar(),
    ).batch_result.total_fill_quantity == 0


def test_active_session_delegates_and_fills_all_three_paths():
    evaluated_at = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)
    level1 = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(evaluated_at),
        rule=_level1_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )
    snapshot = match_attested_snapshot_in_session(
        order=_order(),
        quote=_snapshot_quote(evaluated_at),
        rule=_snapshot_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )
    batch = match_snapshot_batch_in_session(
        request=_batch_request(evaluated_at),
        calendar_evidence=_calendar(),
    )

    assert level1.gate_reason is SessionGateReason.NONE
    assert level1.decision.status is MatchStatus.FILLED
    assert snapshot.gate_reason is SessionGateReason.NONE
    assert snapshot.decision.status is MatchStatus.FILLED
    assert batch.gate_reason is SessionGateReason.NONE
    assert batch.batch_result.total_fill_quantity == 100
    assert tuple(
        allocation.decision.fill_quantity
        for allocation in batch.batch_result.allocations
    ) == (80, 20)


@pytest.mark.parametrize(
    "event_at",
    (
        datetime(2026, 8, 3, 11, 29, tzinfo=SHANGHAI),
        datetime(2026, 8, 3, 12, 0, tzinfo=SHANGHAI),
    ),
)
@pytest.mark.parametrize("mode", ("level1", "snapshot"))
def test_active_evaluation_cannot_fill_prior_or_break_session_event(
    mode: str,
    event_at: datetime,
):
    afternoon = datetime(2026, 8, 3, 13, 1, tzinfo=SHANGHAI)
    if mode == "level1":
        result = match_limit_day_in_session(
            order=_order(),
            quote=_level1_quote(event_at),
            rule=_level1_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )
    else:
        result = match_attested_snapshot_in_session(
            order=_order(),
            quote=_snapshot_quote(event_at),
            rule=_snapshot_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )

    assert result.gate_reason is SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION
    assert result.decision.status is MatchStatus.WAITING
    assert result.decision.fill_quantity == 0
    assert any(
        event_id.startswith("session-gate:")
        for event_id, _ in result.decision.updated_order.applied_events
    )


def test_active_batch_cannot_consume_break_event_or_shared_capacity_later():
    event_at = datetime(2026, 8, 3, 12, 0, tzinfo=SHANGHAI)
    afternoon = datetime(2026, 8, 3, 13, 1, tzinfo=SHANGHAI)
    quote = _snapshot_quote(event_at)
    first = match_snapshot_batch_in_session(
        request=_batch_request(afternoon, quote=quote),
        calendar_evidence=_calendar(),
    )

    assert first.gate_reason is SessionGateReason.EVENT_OUTSIDE_ACTIVE_SESSION
    assert first.batch_result.total_fill_quantity == 0
    assert tuple(
        (
            allocation.shared_cap_before,
            allocation.shared_cap_after,
            allocation.decision.status,
        )
        for allocation in first.batch_result.allocations
    ) == (
        (100, 100, MatchStatus.WAITING),
        (100, 100, MatchStatus.WAITING),
    )

    updated_orders = tuple(
        allocation.decision.updated_order
        for allocation in first.batch_result.allocations
    )
    retry = match_snapshot_batch_in_session(
        request=_batch_request(
            afternoon + timedelta(seconds=1),
            quote=quote,
            orders=updated_orders,
        ),
        calendar_evidence=_calendar(),
    )
    assert retry.gate_reason is SessionGateReason.DUPLICATE_EVENT
    assert retry.batch_result.total_fill_quantity == 0
    assert all(
        allocation.decision.status is MatchStatus.DUPLICATE
        for allocation in retry.batch_result.allocations
    )

    changed = _snapshot_quote(event_at, last_price=Decimal("10.01"))
    with pytest.raises(ValueError, match="different semantics"):
        match_snapshot_batch_in_session(
            request=_batch_request(
                afternoon + timedelta(seconds=1),
                quote=changed,
                orders=updated_orders,
            ),
            calendar_evidence=_calendar(),
        )


@pytest.mark.parametrize("mode", ("level1", "snapshot"))
def test_matcher_applied_event_is_still_replay_checked_by_session_gate(mode: str):
    active = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)
    session_break = datetime(2026, 8, 3, 11, 45, tzinfo=SHANGHAI)
    if mode == "level1":
        rule = LimitDayMatchRule(
            rule_version="level1-session-rule-v1",
            tick_size=Decimal("0.01"),
            quote_max_age=timedelta(days=1),
            visible_volume_participation=Decimal("1"),
            maximum_fill_quantity=40,
            require_complete_price_band=False,
            enforce_price_band_bounds=False,
            block_adverse_limit_lock=False,
        )
        quote = _level1_quote(active)
        first = match_limit_day_in_session(
            order=_order(quantity=100),
            quote=quote,
            rule=rule,
            evaluated_at=active,
            calendar_evidence=_calendar(),
        )
        retry_call = lambda changed_quote: match_limit_day_in_session(
            order=first.decision.updated_order,
            quote=changed_quote,
            rule=rule,
            evaluated_at=session_break,
            calendar_evidence=_calendar(),
        )
        changed = _level1_quote(active, ask_price=Decimal("10.01"))
    else:
        quote = _snapshot_quote(active)
        first = match_attested_snapshot_in_session(
            order=_order(quantity=200),
            quote=quote,
            rule=_snapshot_rule(),
            evaluated_at=active,
            calendar_evidence=_calendar(),
        )
        retry_call = lambda changed_quote: match_attested_snapshot_in_session(
            order=first.decision.updated_order,
            quote=changed_quote,
            rule=_snapshot_rule(),
            evaluated_at=session_break,
            calendar_evidence=_calendar(),
        )
        changed = _snapshot_quote(active, last_price=Decimal("10.01"))

    assert first.decision.status is MatchStatus.PARTIALLY_FILLED
    retry = retry_call(quote)
    assert retry.gate_reason is SessionGateReason.DUPLICATE_EVENT
    assert retry.decision.status is MatchStatus.DUPLICATE
    with pytest.raises(ValueError, match="different semantics"):
        retry_call(changed)


@pytest.mark.parametrize("mode", ("level1", "snapshot"))
def test_inactive_event_is_duplicate_later_and_changed_content_is_rejected(
    mode: str,
):
    pre_open = datetime(2026, 8, 3, 9, 20, tzinfo=SHANGHAI)
    afternoon = datetime(2026, 8, 3, 13, 1, tzinfo=SHANGHAI)
    order = _order()
    if mode == "level1":
        quote = _level1_quote(pre_open)
        first = match_limit_day_in_session(
            order=order,
            quote=quote,
            rule=_level1_rule(),
            evaluated_at=pre_open,
            calendar_evidence=_calendar(),
        )
        retry = match_limit_day_in_session(
            order=first.decision.updated_order,
            quote=quote,
            rule=_level1_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )
        changed = _level1_quote(pre_open, ask_price=Decimal("10.01"))
        call = lambda: match_limit_day_in_session(
            order=first.decision.updated_order,
            quote=changed,
            rule=_level1_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )
    else:
        quote = _snapshot_quote(pre_open)
        first = match_attested_snapshot_in_session(
            order=order,
            quote=quote,
            rule=_snapshot_rule(),
            evaluated_at=pre_open,
            calendar_evidence=_calendar(),
        )
        retry = match_attested_snapshot_in_session(
            order=first.decision.updated_order,
            quote=quote,
            rule=_snapshot_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )
        changed = _snapshot_quote(pre_open, last_price=Decimal("10.01"))
        call = lambda: match_attested_snapshot_in_session(
            order=first.decision.updated_order,
            quote=changed,
            rule=_snapshot_rule(),
            evaluated_at=afternoon,
            calendar_evidence=_calendar(),
        )

    assert retry.gate_reason is SessionGateReason.DUPLICATE_EVENT
    assert retry.decision.status is MatchStatus.DUPLICATE
    assert retry.decision.fill_quantity == 0
    with pytest.raises(ValueError, match="different semantics"):
        call()


def test_closed_session_preserves_existing_expiry_for_all_paths():
    evaluated_at = FINAL_CLOSE
    level1 = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(evaluated_at - timedelta(seconds=1)),
        rule=_level1_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )
    snapshot = match_attested_snapshot_in_session(
        order=_order(),
        quote=_snapshot_quote(evaluated_at - timedelta(seconds=1)),
        rule=_snapshot_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=_calendar(),
    )
    batch = match_snapshot_batch_in_session(
        request=_batch_request(evaluated_at),
        calendar_evidence=_calendar(),
    )

    for result in (level1, snapshot):
        assert result.gate_reason is SessionGateReason.SESSION_CLOSED
        assert result.decision.status is MatchStatus.EXPIRED
        assert result.decision.execution_result is not None
        assert result.decision.execution_result.reason_code == "DAY_EXPIRED"
    assert batch.gate_reason is SessionGateReason.SESSION_CLOSED
    assert all(
        allocation.decision.status is MatchStatus.EXPIRED
        for allocation in batch.batch_result.allocations
    )
    assert batch.batch_result.total_fill_quantity == 0


def test_snapshot_batch_inactive_events_cannot_fill_or_mutate_later():
    pre_open = datetime(2026, 8, 3, 9, 20, tzinfo=SHANGHAI)
    afternoon = datetime(2026, 8, 3, 13, 1, tzinfo=SHANGHAI)
    quote = _snapshot_quote(pre_open)
    first_request = _batch_request(pre_open, quote=quote)
    first = match_snapshot_batch_in_session(
        request=first_request,
        calendar_evidence=_calendar(),
    )
    assert first.batch_result.total_fill_quantity == 0
    assert all(
        allocation.decision.status is MatchStatus.WAITING
        for allocation in first.batch_result.allocations
    )

    updated_orders = tuple(
        allocation.decision.updated_order
        for allocation in first.batch_result.allocations
    )
    active_request = _batch_request(
        afternoon,
        quote=quote,
        orders=updated_orders,
    )
    retry = match_snapshot_batch_in_session(
        request=active_request,
        calendar_evidence=_calendar(),
    )
    assert retry.gate_reason is SessionGateReason.DUPLICATE_EVENT
    assert retry.batch_result.total_fill_quantity == 0
    assert all(
        allocation.decision.status is MatchStatus.DUPLICATE
        for allocation in retry.batch_result.allocations
    )

    changed_quote = _snapshot_quote(
        pre_open,
        last_price=Decimal("10.01"),
    )
    changed_request = _batch_request(
        afternoon,
        quote=changed_quote,
        orders=updated_orders,
    )
    with pytest.raises(ValueError, match="different semantics"):
        match_snapshot_batch_in_session(
            request=changed_request,
            calendar_evidence=_calendar(),
        )

    mixed_request = _batch_request(
        afternoon,
        quote=quote,
        orders=(updated_orders[0], _order("order-3", quantity=80)),
    )
    with pytest.raises(ValueError, match="cannot mix"):
        match_snapshot_batch_in_session(
            request=mixed_request,
            calendar_evidence=_calendar(),
        )


def test_calendar_receipt_is_required_by_default_but_research_is_explicit():
    checksum = _calendar(
        evidence_kind=SessionCalendarEvidenceKind.CONTENT_CHECKSUM_ONLY,
        source_receipt_hash=None,
        quality_status="NOT_ASSESSED",
    )
    evaluated_at = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)
    with pytest.raises(ValueError, match="external calendar receipt"):
        match_limit_day_in_session(
            order=_order(),
            quote=_level1_quote(evaluated_at),
            rule=_level1_rule(),
            evaluated_at=evaluated_at,
            calendar_evidence=checksum,
        )
    research = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(evaluated_at),
        rule=_level1_rule(),
        evaluated_at=evaluated_at,
        calendar_evidence=checksum,
        require_external_receipt=False,
    )
    assert research.decision.status is MatchStatus.FILLED


def test_day_order_must_be_bound_to_same_calendar_window():
    active = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)
    wrong_expiry = _order(
        expires_at=datetime(2026, 8, 4, 15, 0, tzinfo=SHANGHAI)
    )
    with pytest.raises(ValueError, match="final session close"):
        match_limit_day_in_session(
            order=wrong_expiry,
            quote=_level1_quote(active),
            rule=_level1_rule(),
            evaluated_at=active,
            calendar_evidence=_calendar(),
        )

    break_earliest = _order(
        earliest_at=datetime(2026, 8, 3, 12, 0, tzinfo=SHANGHAI)
    )
    with pytest.raises(ValueError, match="inside a calendar execution session"):
        match_attested_snapshot_in_session(
            order=break_earliest,
            quote=_snapshot_quote(active),
            rule=_snapshot_rule(),
            evaluated_at=active,
            calendar_evidence=_calendar(),
        )

    bad_batch = _batch_request(active, orders=(wrong_expiry,))
    with pytest.raises(ValueError, match="final session close"):
        match_snapshot_batch_in_session(
            request=bad_batch,
            calendar_evidence=_calendar(),
        )


def test_exact_calendar_contract_and_output_immutability():
    active = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)

    class ForgedCalendar(TradingSessionCalendarEvidence):
        pass

    with pytest.raises(TypeError, match="exactly TradingSessionCalendarEvidence"):
        match_limit_day_in_session(
            order=_order(),
            quote=_level1_quote(active),
            rule=_level1_rule(),
            evaluated_at=active,
            calendar_evidence=object.__new__(ForgedCalendar),
        )

    result = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(active),
        rule=_level1_rule(),
        evaluated_at=active,
        calendar_evidence=_calendar(),
    )
    assert type(result) is SessionGatedMatchDecision
    with pytest.raises(FrozenInstanceError):
        result.gate_reason = SessionGateReason.SESSION_BREAK  # type: ignore[misc]


def test_gate_outputs_reject_non_active_fill_envelopes_and_hash_tampering():
    active_at = datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI)
    active = match_limit_day_in_session(
        order=_order(),
        quote=_level1_quote(active_at),
        rule=_level1_rule(),
        evaluated_at=active_at,
        calendar_evidence=_calendar(),
    )
    closed = match_limit_day_in_session(
        order=_order("closed-order"),
        quote=_level1_quote(FINAL_CLOSE - timedelta(seconds=1)),
        rule=_level1_rule(),
        evaluated_at=FINAL_CLOSE,
        calendar_evidence=_calendar(),
    )
    with pytest.raises(ValueError, match="only an ACTIVE session"):
        SessionGatedMatchDecision(
            mode=active.mode,
            assessment=closed.assessment,
            gate_reason=SessionGateReason.SESSION_CLOSED,
            decision=active.decision,
        )

    active_batch = match_snapshot_batch_in_session(
        request=_batch_request(active_at),
        calendar_evidence=_calendar(),
    )
    with pytest.raises(ValueError, match="only an ACTIVE session"):
        SessionGatedSnapshotBatchDecision(
            assessment=closed.assessment,
            gate_reason=SessionGateReason.SESSION_CLOSED,
            batch_result=active_batch.batch_result,
        )

    forged = replace(active)
    object.__setattr__(forged, "decision_hash", "f" * 64)
    with pytest.raises(ValueError, match="fields or hash were tampered"):
        gate_module.validate_session_gated_match_decision(forged)

    forged_batch = replace(active_batch)
    object.__setattr__(forged_batch, "decision_hash", "f" * 64)
    with pytest.raises(ValueError, match="fields or hash were tampered"):
        gate_module.validate_session_gated_snapshot_batch_decision(forged_batch)


def test_session_gate_has_no_write_strategy_account_or_v2_adapter_imports():
    tree = ast.parse(inspect.getsource(gate_module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    forbidden = (
        "sqlalchemy",
        "repository",
        "ledger",
        "account",
        "broker",
        "strategy",
        "server.integrations",
        "v2_execution_adapter",
    )
    assert not any(
        fragment in module
        for module in imported
        for fragment in forbidden
    )
