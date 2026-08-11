from __future__ import annotations

from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    FillExecutionEvidence,
    QuoteReceiptEvidence,
    QuoteReceiptType,
)
from server.integrations.v2_execution_evidence_writer import writer as writer_impl
from tools.trading_v2_evidence_behavioral_scenario import (
    build_behavioral_scenario,
    build_conflicting_double_writer_scenario,
)


def test_behavioral_scenario_is_deterministic_and_explicitly_five_table() -> None:
    first = build_behavioral_scenario()
    second = build_behavioral_scenario()

    assert tuple(case.evidence_type for case in first.cases) == (
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
    )
    assert tuple(case.primary_value for case in first.cases) == tuple(
        case.primary_value for case in second.cases
    )
    assert all(len(case.primary_value) == 64 for case in first.cases)
    assert {seed.table for seed in first.seed_rows} == {
        "st_trade_account_v2",
        "st_order_v2",
        "st_cash_ledger_v2",
        "st_quote_event_v2",
        "st_fill_v2",
        "st_fee_profile_v2",
        "st_instrument_rule_v2",
    }
    assert sum(seed.table == "st_order_v2" for seed in first.seed_rows) == 2


def test_quote_and_fill_use_safe_deterministic_dependencies() -> None:
    scenario = build_behavioral_scenario()
    by_type = {case.evidence_type: case for case in scenario.cases}

    quote = by_type["QUOTE_RECEIPT"].evidence
    fill = by_type["FILL_EXECUTION"].evidence

    assert type(quote) is QuoteReceiptEvidence
    assert quote.receipt_type is QuoteReceiptType.OTHER
    assert quote.provenance.authority_status is AuthorityStatus.CONTENT_HASH_ONLY
    assert type(fill) is FillExecutionEvidence
    assert fill.quote_evidence is quote
    assert fill.calendar_evidence is by_type["MARKET_CALENDAR"].evidence
    assert fill.order_id != by_type["ORDER_TRANSITION"].evidence.order_id
    assert by_type["FILL_EXECUTION"].rollback_dependencies == (
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
    )
    assert all(
        not case.rollback_dependencies
        for case in scenario.cases
        if case.evidence_type != "FILL_EXECUTION"
    )


def test_quote_and_fill_payloads_exactly_project_seeded_v2_rows() -> None:
    scenario = build_behavioral_scenario()
    rows: dict[str, list[dict[str, object]]] = {}
    for seed in scenario.seed_rows:
        rows.setdefault(seed.table, []).append(dict(seed.values))
    by_type = {case.evidence_type: case for case in scenario.cases}
    quote = by_type["QUOTE_RECEIPT"].evidence
    fill = by_type["FILL_EXECUTION"].evidence
    assert type(quote) is QuoteReceiptEvidence
    assert type(fill) is FillExecutionEvidence
    fill_order = next(
        row for row in rows["st_order_v2"] if row["order_id"] == fill.order_id
    )

    assert CanonicalJson.from_value(
        writer_impl._canonical_quote_row(rows["st_quote_event_v2"][0])
    ).value() == quote.receipt_payload.value()["quote_row"]
    assert CanonicalJson.from_value(
        writer_impl._canonical_order_payload(fill_order)
    ).json_text == fill.order_payload.json_text
    assert CanonicalJson.from_value(
        writer_impl._canonical_fill_payload(rows["st_fill_v2"][0])
    ).json_text == fill.fill_payload.json_text
    assert CanonicalJson.from_value(
        writer_impl._canonical_fee_payload(rows["st_fee_profile_v2"][0])
    ).json_text == fill.fee_schedule.json_text
    assert CanonicalJson.from_value(
        writer_impl._canonical_rule_payload(rows["st_instrument_rule_v2"][0])
    ).json_text == fill.instrument_rule.json_text


def test_behavioral_scenario_never_claims_external_authority() -> None:
    scenario = build_behavioral_scenario()

    for case in scenario.cases:
        assert (
            case.evidence.provenance.authority_status
            is AuthorityStatus.CONTENT_HASH_ONLY
        )
        assert case.evidence.provenance.authority_receipt_hash is None


def test_conflicting_double_writer_scenario_has_five_legal_distinct_pairs() -> None:
    base = build_behavioral_scenario()
    first = build_conflicting_double_writer_scenario(base)
    second = build_conflicting_double_writer_scenario(base)

    assert tuple(pair.evidence_type for pair in first.pairs) == (
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
    )
    assert tuple(
        (pair.left.primary_value, pair.right.primary_value)
        for pair in first.pairs
    ) == tuple(
        (pair.left.primary_value, pair.right.primary_value)
        for pair in second.pairs
    )
    for pair in first.pairs:
        assert pair.left.primary_value != pair.right.primary_value
        assert pair.left.evidence_type == pair.right.evidence_type
        assert pair.left.table == pair.right.table == pair.table
        assert pair.left.primary_column == pair.right.primary_column
        assert pair.natural_key_columns
        assert len(pair.natural_key_columns) == len(pair.natural_key_values)
        assert (
            pair.left.evidence.provenance.authority_status
            is AuthorityStatus.CONTENT_HASH_ONLY
        )
        assert (
            pair.right.evidence.provenance.authority_status
            is AuthorityStatus.CONTENT_HASH_ONLY
        )
    assert {seed.table for seed in first.seed_rows} == {
        "st_trade_account_v2",
        "st_order_v2",
        "st_cash_ledger_v2",
        "st_quote_event_v2",
        "st_fill_v2",
    }
