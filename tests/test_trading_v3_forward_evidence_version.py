from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal

import pytest

from server.trading_v3.forward_evidence import (
    ATTRIBUTION_VERSION,
    EXIT_ALLOCATION_PROTOCOL,
    INTENT_EPISODE_PROTOCOL,
    _assert_persisted_exit_allocations,
    _ownership_hash,
    _require_valid_dynamic_shadow_binding,
    _sample_owner,
    intent_episode_id,
    primary_strategy_version,
    reconstruct_executed_forward_records,
)
from server.engine.strategy_governance import (
    _aggregate_forward_intent_episodes,
    calculate_return_metrics,
)


RUN_UID = "run-version-chain-1"
MODEL_VERSION = "trading_v3.11.0-paper"
STRATEGY_KEY = "main_wave"
STRATEGY_VERSION = f"{MODEL_VERSION}:{STRATEGY_KEY}"
FORECAST_ID = "forecast-version-chain-1"
STOCK_CODE = "600000"


def _buy_row(*, evidence_overrides: dict[str, object] | None = None):
    payload: dict[str, object] = {
        "run_uid": RUN_UID,
        "model_version": MODEL_VERSION,
        "supporting_strategy_keys": [STRATEGY_KEY],
        "primary_strategy_key": STRATEGY_KEY,
        "primary_strategy_version": STRATEGY_VERSION,
        "primary_forecast_id": FORECAST_ID,
        "sample_owner_role": "PRIMARY",
        "attribution_version": ATTRIBUTION_VERSION,
        "ownership_hash": _ownership_hash(
            RUN_UID,
            FORECAST_ID,
            STOCK_CODE,
            STRATEGY_KEY,
            STRATEGY_VERSION,
        ),
    }
    payload.update(evidence_overrides or {})
    return {
        "fill_id": "buy-fill-1",
        "order_id": "buy-order-1",
        "intent_id": "intent-1",
        "decision_run_uid": RUN_UID,
        "intent_reason_code": "V3_PAPER_DISCOVERY",
        "evidence_json": json.dumps(payload),
        "stock_code": STOCK_CODE,
        "side": "BUY",
        "quantity": 100,
        "price": "10.00",
        "gross_amount": "1000.00",
        "fee_amount": "1.00",
        "filled_at": datetime(2026, 8, 21, 9, 31),
    }


def _sell_row():
    return {
        "fill_id": "sell-fill-1",
        "order_id": "sell-order-1",
        "stock_code": STOCK_CODE,
        "side": "SELL",
        "quantity": 100,
        "price": "11.00",
        "gross_amount": "1100.00",
        "fee_amount": "1.00",
        "intent_reason_code": "V3_EXIT",
        "filled_at": datetime(2026, 8, 22, 14, 50),
    }


def _forecast_ids():
    return {(RUN_UID, STOCK_CODE, STRATEGY_KEY): FORECAST_ID}


def test_primary_strategy_version_is_exact_and_fail_closed():
    assert primary_strategy_version(MODEL_VERSION, STRATEGY_KEY) == (
        STRATEGY_VERSION
    )
    with pytest.raises(ValueError, match="requires model_version"):
        primary_strategy_version("", STRATEGY_KEY)
    with pytest.raises(ValueError, match="exceeds 160"):
        primary_strategy_version("m" * 100, "s" * 100)


def test_forward_owner_binds_relational_run_model_and_strategy_version():
    owner, rejection = _sample_owner(
        _buy_row(),
        _forecast_ids(),
        {RUN_UID: MODEL_VERSION},
    )

    assert rejection == ""
    assert owner is not None
    assert owner["strategy_key"] == STRATEGY_KEY
    assert owner["strategy_version"] == STRATEGY_VERSION
    assert owner["source_run_uid"] == RUN_UID
    assert owner["source_forecast_id"] == FORECAST_ID

    missing_intent = _buy_row()
    missing_intent["intent_id"] = ""
    missing_owner, missing_rejection = _sample_owner(
        missing_intent,
        _forecast_ids(),
        {RUN_UID: MODEL_VERSION},
    )
    assert missing_owner is None
    assert missing_rejection == "SOURCE_INTENT_ID_MISSING"


def test_runtime_registry_owner_uses_hash_verified_governance_version():
    receipt_payload = {
        "schema": "probiga.governance-paper-buy-receipt.v1",
        "strategy_key": STRATEGY_KEY,
        "strategy_version": "registry-v7",
        "strategy_version_hash": "1" * 64,
        "strategy_source_kind": "runtime_registry",
        "real_order_authority": False,
    }
    receipt = {
        **receipt_payload,
        "receipt_hash": hashlib.sha256(json.dumps(
            receipt_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest(),
    }
    owner, rejection = _sample_owner(
        _buy_row(evidence_overrides={
            "primary_strategy_version": "registry-v7",
            "ownership_hash": _ownership_hash(
                RUN_UID,
                FORECAST_ID,
                STOCK_CODE,
                STRATEGY_KEY,
                "registry-v7",
            ),
            "strategy_governance": receipt,
        }),
        _forecast_ids(),
        {RUN_UID: MODEL_VERSION},
    )
    assert rejection == ""
    assert owner is not None
    assert owner["strategy_version"] == "registry-v7"

    forged = {**receipt, "strategy_version": "registry-v8"}
    forged_owner, forged_rejection = _sample_owner(
        _buy_row(evidence_overrides={
            "primary_strategy_version": "registry-v8",
            "strategy_governance": forged,
        }),
        _forecast_ids(),
        {RUN_UID: MODEL_VERSION},
    )
    assert forged_owner is None
    assert forged_rejection == "RUNTIME_GOVERNANCE_RECEIPT_INVALID"


@pytest.mark.parametrize(
    ("evidence_overrides", "run_model_versions", "expected_rejection"),
    (
        (
            {"primary_strategy_version": f"other:{STRATEGY_KEY}"},
            {RUN_UID: MODEL_VERSION},
            "PRIMARY_STRATEGY_VERSION_MISMATCH",
        ),
        (
            {"primary_strategy_version": STRATEGY_VERSION.upper()},
            {RUN_UID: MODEL_VERSION},
            "PRIMARY_STRATEGY_VERSION_MISMATCH",
        ),
        (
            {
                "primary_strategy_key": "",
                "primary_strategy_version": STRATEGY_VERSION,
            },
            {RUN_UID: MODEL_VERSION},
            "PRIMARY_STRATEGY_KEY_MISSING",
        ),
        (
            {
                "primary_strategy_key": "",
                "primary_strategy_version": "",
            },
            {RUN_UID: MODEL_VERSION},
            "PRIMARY_STRATEGY_KEY_MISSING",
        ),
        (
            {"model_version": "other-model"},
            {RUN_UID: MODEL_VERSION},
            "INTENT_MODEL_VERSION_MISMATCH",
        ),
        (
            {},
            {},
            "RUN_MODEL_VERSION_NOT_FOUND",
        ),
    ),
)
def test_forward_owner_quarantines_mismatched_or_ambiguous_intents(
    evidence_overrides,
    run_model_versions,
    expected_rejection,
):
    owner, rejection = _sample_owner(
        _buy_row(evidence_overrides=evidence_overrides),
        _forecast_ids(),
        run_model_versions,
    )

    assert owner is None
    assert rejection == expected_rejection


def test_reconstruction_persists_exact_version_and_derives_legacy_version():
    diagnostics: dict[str, int] = {}
    records = reconstruct_executed_forward_records(
        (_buy_row(), _sell_row()),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        diagnostics=diagnostics,
    )

    assert diagnostics == {}
    assert len(records) == 1
    assert records[0]["evidence_status"] == "MATURED"
    assert records[0]["strategy_key"] == STRATEGY_KEY
    assert records[0]["strategy_version"] == STRATEGY_VERSION
    assert records[0]["exit_allocations"] == [{
        "allocation_id": records[0]["exit_allocations"][0]["allocation_id"],
        "evidence_id": records[0]["evidence_id"],
        "attribution_status": "ATTRIBUTED",
        "account_id": "",
        "stock_code": STOCK_CODE,
        "entry_fill_id": "buy-fill-1",
        "exit_fill_id": "sell-fill-1",
        "exit_order_id": "sell-order-1",
        "allocation_sequence": 0,
        "allocated_quantity": 100,
        "allocated_gross_cny": records[0]["exit_gross_cny"],
        "allocated_fee_cny": records[0]["exit_fee_cny"],
        "exit_filled_at": datetime(2026, 8, 22, 14, 50),
        "allocation_protocol_version": EXIT_ALLOCATION_PROTOCOL,
    }]

    old_diagnostics: dict[str, int] = {}
    old_records = reconstruct_executed_forward_records(
        (
            _buy_row(evidence_overrides={"primary_strategy_version": ""}),
            _sell_row(),
        ),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        diagnostics=old_diagnostics,
    )
    assert old_diagnostics == {}
    assert len(old_records) == 1
    assert old_records[0]["strategy_version"] == STRATEGY_VERSION
    assert old_records[0]["attribution_status"] == "LEGACY_VERSION_DERIVED"


def test_one_sell_fill_is_normalized_across_two_fifo_entry_evidences():
    second_buy = _buy_row()
    second_buy.update({
        "fill_id": "buy-fill-2",
        "order_id": "buy-order-2",
        "intent_id": "intent-2",
        "filled_at": datetime(2026, 8, 21, 9, 32),
    })
    sell = _sell_row()
    sell.update({
        "quantity": 200,
        "gross_amount": "2200.00",
        "fee_amount": "2.00",
    })

    records = reconstruct_executed_forward_records(
        (_buy_row(), second_buy, sell),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
    )

    assert len(records) == 2
    allocations = [
        allocation
        for record in records
        for allocation in record["exit_allocations"]
    ]
    assert [item["allocated_quantity"] for item in allocations] == [100, 100]
    assert {item["exit_fill_id"] for item in allocations} == {"sell-fill-1"}
    assert len({item["allocation_id"] for item in allocations}) == 2
    assert sum(item["allocated_gross_cny"] for item in allocations) == 2200
    assert sum(item["allocated_fee_cny"] for item in allocations) == 2
    assert sum(record["closed_quantity"] for record in records) == 200


def test_six_fifo_lots_assign_rounding_tail_and_conserve_raw_amounts():
    buys = []
    for index in range(6):
        buy = _buy_row()
        buy.update({
            "fill_id": f"buy-fill-{index}",
            "order_id": f"buy-order-{index}",
            "intent_id": f"intent-{index}",
            "quantity": 1,
            "gross_amount": "10.00",
            "fee_amount": "0.01",
            "filled_at": datetime(2026, 8, 21, 9, 31, index),
        })
        buys.append(buy)
    sell = _sell_row()
    sell.update({
        "quantity": 6,
        "price": "1.666667",
        "gross_amount": "10.00",
        "fee_amount": "0.01",
    })
    allocations: list[dict] = []

    records = reconstruct_executed_forward_records(
        (*buys, sell),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        allocation_rows=allocations,
    )

    assert len(records) == 6
    assert len(allocations) == 6
    assert sum(item["allocated_quantity"] for item in allocations) == 6
    assert sum(item["allocated_gross_cny"] for item in allocations) == 10
    assert sum(item["allocated_fee_cny"] for item in allocations) == Decimal(
        "0.010000"
    )
    assert allocations[-1]["allocated_gross_cny"] == Decimal("1.666665")
    assert allocations[-1]["allocated_fee_cny"] == Decimal("0.001665")


def test_eighty_partial_fills_of_one_intent_are_one_cash_return_sample():
    buys = []
    for index in range(80):
        buy = _buy_row()
        buy.update({
            "fill_id": f"buy-fill-{index:03d}",
            "quantity": 1,
            "gross_amount": "10.00",
            "fee_amount": "0.01",
            "filled_at": datetime(2026, 8, 21, 9, 31, index % 60),
        })
        buys.append(buy)
    sell = _sell_row()
    sell.update({
        "quantity": 80,
        "gross_amount": "880.00",
        "fee_amount": "0.80",
    })

    fill_facts = reconstruct_executed_forward_records(
        (*buys, sell),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
    )
    assert len(fill_facts) == 80
    for fact in fill_facts:
        fact.update({
            "account_id": "paper-main-v2",
            "entry_cash_binding_count": 1,
            "source_intent_buy_fill_count": 80,
            "source_intent_entry_quantity": 80,
            "source_intent_entry_gross_cny": Decimal("800.00"),
            "source_intent_entry_fee_cny": Decimal("0.80"),
            "return_pct": fact["realized_net_return_pct"],
        })

    episodes = _aggregate_forward_intent_episodes(fill_facts)

    assert len(episodes) == 1
    assert episodes[0]["episode_protocol"] == INTENT_EPISODE_PROTOCOL
    assert episodes[0]["episode_id"] == intent_episode_id(
        "intent-1", STRATEGY_VERSION,
    )
    assert episodes[0]["episode_member_fill_count"] == 80
    assert episodes[0]["entry_quantity"] == 80
    expected_return = (
        Decimal("880.00") - Decimal("0.80")
        - Decimal("800.00") - Decimal("0.80")
    ) / Decimal("800.80") * Decimal("100")
    assert Decimal(str(episodes[0]["return_pct"])) == pytest.approx(
        expected_return,
    )
    metrics = calculate_return_metrics(episodes, window_days=120)
    assert metrics["completed_trades"] == 1

    with pytest.raises(ValueError, match="成交全集认证"):
        _aggregate_forward_intent_episodes(fill_facts[:-1])
    conflicting_version = [dict(item) for item in fill_facts]
    conflicting_version[-1]["strategy_version"] = "other-version"
    with pytest.raises(ValueError, match="不同账户、版本、策略"):
        _aggregate_forward_intent_episodes(conflicting_version)


def test_mixed_attributed_and_unattributed_fifo_lots_cover_one_sell():
    unattributed = _buy_row()
    unattributed.update({
        "fill_id": "legacy-buy-fill",
        "order_id": "legacy-buy-order",
        "intent_id": "legacy-intent",
        "intent_reason_code": "LEGACY_ENTRY",
        "evidence_json": "{}",
    })
    attributed = _buy_row()
    attributed.update({
        "fill_id": "v3-buy-fill",
        "order_id": "v3-buy-order",
        "intent_id": "v3-intent",
        "filled_at": datetime(2026, 8, 21, 9, 32),
    })
    sell = _sell_row()
    sell.update({
        "quantity": 200,
        "gross_amount": "2200.00",
        "fee_amount": "2.00",
    })
    allocations: list[dict] = []

    records = reconstruct_executed_forward_records(
        (unattributed, attributed, sell),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        allocation_rows=allocations,
    )

    assert len(records) == 1
    assert [item["attribution_status"] for item in allocations] == [
        "UNATTRIBUTED", "ATTRIBUTED",
    ]
    assert allocations[0]["evidence_id"] is None
    assert allocations[1]["evidence_id"] == records[0]["evidence_id"]
    assert sum(item["allocated_quantity"] for item in allocations) == 200
    assert sum(item["allocated_gross_cny"] for item in allocations) == 2200
    assert sum(item["allocated_fee_cny"] for item in allocations) == 2


def test_sell_without_complete_fifo_inventory_is_explicitly_rejected():
    sell = _sell_row()
    sell.update({
        "quantity": 200,
        "gross_amount": "2200.00",
        "fee_amount": "2.00",
    })
    diagnostics: dict[str, int] = {}
    allocations: list[dict] = []

    records = reconstruct_executed_forward_records(
        (_buy_row(), sell),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        diagnostics=diagnostics,
        allocation_rows=allocations,
    )

    assert diagnostics["SELL_FIFO_COVERAGE_GAP"] == 1
    assert allocations == []
    assert records[0]["evidence_status"] == "OPEN"
    assert records[0]["closed_quantity"] == 0
    assert records[0]["exit_gross_cny"] == 0
    assert records[0]["exit_fee_cny"] == 0


def test_persisted_allocation_replay_rejects_missing_or_tampered_rows():
    allocations: list[dict] = []
    reconstruct_executed_forward_records(
        (_buy_row(), _sell_row()),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        allocation_rows=allocations,
    )
    for item in allocations:
        item["account_id"] = "paper-main-v2"

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return iter(self._rows)

    class _Connection:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, _statement, _params):
            return _Result(self.rows)

    assert _assert_persisted_exit_allocations(
        _Connection([dict(allocations[0])]),
        allocations,
        account_id="paper-main-v2",
    ) == 1
    with pytest.raises(RuntimeError, match="deterministic FIFO replay"):
        _assert_persisted_exit_allocations(
            _Connection([]),
            allocations,
            account_id="paper-main-v2",
        )
    tampered = dict(allocations[0])
    tampered["allocated_quantity"] = 99
    with pytest.raises(RuntimeError, match="deterministic FIFO replay"):
        _assert_persisted_exit_allocations(
            _Connection([tampered]),
            allocations,
            account_id="paper-main-v2",
        )


def test_unattributed_fill_identity_is_distinct_across_rejection_reasons():
    invalid = _buy_row(evidence_overrides={"primary_strategy_key": ""})
    diagnostics = {"DYNAMIC_SHADOW_BOOTSTRAP_AUTHORIZATION_INVALID": 1}
    rejected_fill_ids = {str(invalid["fill_id"])}

    records = reconstruct_executed_forward_records(
        (invalid,),
        forecast_ids=_forecast_ids(),
        run_model_versions={RUN_UID: MODEL_VERSION},
        diagnostics=diagnostics,
        diagnostic_fill_ids=rejected_fill_ids,
    )

    assert records == []
    assert sum(diagnostics.values()) == 2
    assert rejected_fill_ids == {"buy-fill-1"}


@pytest.mark.parametrize("status", ["INVALID", "UNAVAILABLE_OR_INVALID", ""])
def test_dynamic_shadow_binding_failure_cannot_be_reported_as_success(status):
    with pytest.raises(RuntimeError, match="failed closed"):
        _require_valid_dynamic_shadow_binding({"status": status})


def test_counterfactual_worker_stops_before_writes_on_forward_integrity_failure(
    monkeypatch,
):
    from server.trading_v3 import counterfactual_worker as worker

    pending_called = False

    def _failed_sync(*_args, **_kwargs):
        raise RuntimeError("dynamic shadow evidence binding failed closed")

    def _pending(*_args, **_kwargs):
        nonlocal pending_called
        pending_called = True
        return []

    monkeypatch.setattr(worker, "sync_executed_forward_evidence", _failed_sync)
    monkeypatch.setattr(worker, "_pending_forecasts", _pending)

    with pytest.raises(RuntimeError, match="failed closed"):
        worker.run_counterfactual_audit(object(), object())
    assert pending_called is False
