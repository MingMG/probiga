from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from server.engine import strategy_governance as governance
from server.engine.strategy_funding_checkpoint import (
    FUNDING_CHECKPOINT_SCHEMA,
    FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
    FUNDING_DAILY_FACT_SCHEMA,
    _normalize_check,
    canonical_hash,
    canonical_json,
    checkpoint_identity,
    funding_daily_fact_hash,
    funding_daily_fact_identity,
    ordered_funding_fact_set_hash,
)


def test_canonical_json_is_strict_deterministic_and_mysql_sha_compatible():
    value = {"中文": "资金", "amount": Decimal("10.2500"), "ok": True}
    encoded = canonical_json(value)
    assert encoded == '{"amount":"10.2500","ok":true,"中文":"资金"}'
    assert canonical_hash(value) == hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    for invalid in (float("nan"), float("inf"), Decimal("NaN"), object(), (1, 2)):
        with pytest.raises((TypeError, ValueError)):
            canonical_json(invalid)


def test_mysql84_check_normalization_preserves_boolean_semantics_and_literals():
    real_mysql84 = (
        r"((`replay_mode` = _utf8mb4\'FULL_BOOTSTRAP\') and "
        r"(`replay_session_count` >= 1))"
    )
    declared = "replay_mode = 'FULL_BOOTSTRAP' AND replay_session_count >= 1"
    assert _normalize_check(real_mysql84) == _normalize_check(declared)
    assert _normalize_check(real_mysql84.replace("FULL_BOOTSTRAP", "LIVE")) != (
        _normalize_check(declared)
    )
    assert _normalize_check("a BETWEEN 1 AND 3 AND b = 0") == _normalize_check(
        "(a BETWEEN 1 AND 3) AND (b = 0)"
    )


def _fact_row(
    *, day: str, run_uid: str, previous_id: str = "",
    previous_hash: str = "", depth: int,
) -> tuple[dict, dict]:
    origin_checkpoint_id = checkpoint_identity(
        strategy_key="alpha",
        strategy_version="v1",
        account_id="paper-main-v2",
        trade_date=day,
        anchor_run_uid=run_uid,
    )
    fact_id = funding_daily_fact_identity(
        entity_type="STRATEGY",
        entity_key="alpha",
        entity_version="v1",
        account_id="paper-main-v2",
        trade_date=day,
        anchor_run_uid=run_uid,
    )
    payload = {
        "schema": FUNDING_DAILY_FACT_SCHEMA,
        "entity_type": "STRATEGY",
        "entity_key": "alpha",
        "entity_version": "v1",
        "entity_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-main-v2",
        "trade_date": day,
        "origin_checkpoint_id": origin_checkpoint_id,
        "previous_fact_id": previous_id,
        "previous_fact_hash": previous_hash,
        "opening_cash_cny": "1000.000000",
        "closing_cash_cny": "1000.000000",
        "opening_equity_cny": "1000.000000",
        "closing_equity_cny": "1000.000000",
        "normalized_opening_equity": "100.00000000",
        "normalized_closing_equity": "100.00000000",
        "normalization_base_equity_cny": "1000.000000",
        "daily_return_pct": "0.000000000000",
        "actual_cost_pct": "0.000000000000",
        "opening_cumulative_fee_cny": "0.000000",
        "daily_fee_cny": "0.000000",
        "cumulative_fee_cny": "0.000000",
        "high_watermark_equity_cny": "1000.000000",
        "stock_risk_exposure": {},
        "closed_evidence_ids": [],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    fact_hash = funding_daily_fact_hash(payload)
    row = {
        "chain_depth": depth,
        "fact_id": fact_id,
        "fact_hash": fact_hash,
        "fact_json": canonical_json(payload),
        "entity_type": "STRATEGY",
        "entity_key": "alpha",
        "entity_version": "v1",
        "entity_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-main-v2",
        "trade_date": day,
        "origin_checkpoint_id": origin_checkpoint_id,
        "previous_fact_id": previous_id or None,
        "previous_fact_hash": previous_hash or None,
        "opening_cash_cny": Decimal("1000"),
        "closing_cash_cny": Decimal("1000"),
        "opening_equity_cny": Decimal("1000"),
        "closing_equity_cny": Decimal("1000"),
        "daily_return_pct": Decimal("0"),
        "cumulative_fee_cny": Decimal("0"),
        "high_watermark_equity_cny": Decimal("1000"),
        "stock_exposure_json": "{}",
        "closed_evidence_ids_json": "[]",
        "anchor_run_uid": run_uid,
    }
    return row, payload


def _origin_context(rows: list[dict]) -> dict:
    checkpoints = {}
    runs = {}
    audits = {}
    batch_members = {}
    for index, row in enumerate(rows, start=1):
        checkpoint_id = row["origin_checkpoint_id"]
        checkpoint_hash = canonical_hash({"checkpoint": checkpoint_id})
        chain_hash = canonical_hash({"chain": checkpoint_id})
        audit_id = f"{index:032x}"
        audit_hash_placeholder = "0" * 64
        checkpoint = {
            "checkpoint_id": checkpoint_id,
            "strategy_key": "alpha",
            "strategy_version": "v1",
            "strategy_version_hash": "a" * 64,
            "execution_binding_hash": "b" * 64,
            "account_id": "paper-main-v2",
            "trade_date": row["trade_date"],
            "replay_mode": "BOUNDED_INCREMENTAL",
            "replay_session_count": 1,
            "max_holding_days": 20,
            "checkpoint_hash": checkpoint_hash,
            "chain_hash": chain_hash,
            "history_fact_count": 1,
            "history_fact_set_hash": ordered_funding_fact_set_hash([row]),
            "history_tip_fact_id": row["fact_id"],
            "history_tip_fact_hash": row["fact_hash"],
            "new_fact_count": 1,
            "new_fact_set_hash": ordered_funding_fact_set_hash([row]),
            "new_fact_first_id": row["fact_id"],
            "new_fact_tip_id": row["fact_id"],
            "anchor_run_uid": row["anchor_run_uid"],
            "canonical_result_hash": "",
            "anchor_audit_id": audit_id,
            "anchor_audit_hash": audit_hash_placeholder,
            "automatic_real_order_submission": 0,
            "real_order_authority": 0,
        }
        checkpoint_entry = governance._funding_checkpoint_manifest_entry(
            checkpoint
        )
        current_entities = [{
            "entity_type": "STRATEGY",
            "entity_key": "alpha",
            "entity_version": "v1",
        }]
        coverage = {
            "current_entity_count": 1,
            "eligible_count": 1,
            "checkpointed_count": 1,
            "ineligible_count": 0,
            "current_entity_set_hash": governance._funding_entity_set_hash(
                current_entities
            ),
            "checkpointed_set_hash": governance._funding_entity_set_hash(
                current_entities
            ),
            "ineligible_set_hash": governance._funding_entity_set_hash([]),
            "eligible_persistence_coverage_pct": 100.0,
        }
        checkpoint_root = governance._funding_manifest_batch_root(
            [checkpoint_entry], kind="CHECKPOINT"
        )
        ineligible_root = governance._funding_manifest_batch_root(
            [], kind="INELIGIBLE"
        )
        combination_recipe_root = governance._funding_manifest_batch_root(
            [], kind="COMBINATION_RECIPE"
        )
        manifest_payload = {
            "schema": "probiga.strategy-funding-checkpoint-manifest.v2",
            "run_uid": row["anchor_run_uid"],
            "trade_date": row["trade_date"],
            "coverage": coverage,
            "checkpoint_root": checkpoint_root,
            "combination_recipe_root": combination_recipe_root,
            "ineligible_root": ineligible_root,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        manifest = {
            **manifest_payload,
            "manifest_hash": canonical_hash(manifest_payload),
        }
        result = {
            "run_uid": row["anchor_run_uid"],
            "status": "ok",
            "is_canonical": True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "funding_checkpoint_manifest": manifest,
        }
        result_json = canonical_json(result)
        result_hash = hashlib.sha256(result_json.encode()).hexdigest()
        checkpoint["canonical_result_hash"] = result_hash
        evidence = {
            "schema": governance.FUNDING_CHECKPOINT_AUDIT_SCHEMA,
            "run_uid": row["anchor_run_uid"],
            "canonical_result_hash": result_hash,
            "checkpoint_manifest_hash": manifest["manifest_hash"],
            "coverage": coverage,
            "checkpoint_root": checkpoint_root,
            "combination_recipe_root": combination_recipe_root,
            "ineligible_root": ineligible_root,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        audit_payload = {
            "entity_type": "SYSTEM",
            "entity_key": "strategy_funding_checkpoint_manifest",
            "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
            "reason": "test",
            "operator": "pytest",
            "before": {},
            "after": {},
            "evidence": evidence,
            "nonce": f"{index + 100:032x}",
        }
        audit_hash = canonical_hash(audit_payload)
        checkpoint["anchor_audit_hash"] = audit_hash
        row.update({
            "canonical_result_hash": result_hash,
            "anchor_audit_id": audit_id,
            "anchor_audit_hash": audit_hash,
        })
        checkpoints[checkpoint_id] = checkpoint
        runs[row["anchor_run_uid"]] = {
            "run_uid": row["anchor_run_uid"],
            "status": "COMPLETED",
            "is_canonical": 1,
            "result_hash": result_hash,
            "result_json_hash_valid": 1,
            "result_json_bytes": len(result_json.encode("utf-8")),
            "result_run_uid": row["anchor_run_uid"],
            "result_status": "ok",
            "result_is_canonical": "true",
            "result_automatic_real_order_submission": "false",
            "result_real_order_authority": "false",
            "result_manifest_schema": manifest["schema"],
            "result_manifest_hash": manifest["manifest_hash"],
            "result_manifest_run_uid": row["anchor_run_uid"],
            "result_manifest_trade_date": row["trade_date"],
            "result_manifest_automatic_real_order_submission": "false",
            "result_manifest_real_order_authority": "false",
            "_funding_checkpoint_total_count": 1,
            "_funding_checkpoint_batches": {
                checkpoint_id: {
                    "batch_index": 0,
                    "entries": [checkpoint_entry],
                },
            },
        }
        audits[audit_id] = {
            "audit_id": audit_id,
            "entity_type": "SYSTEM",
            "entity_key": "strategy_funding_checkpoint_manifest",
            "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
            "reason": "test",
            "operator_name": "pytest",
            "before_json": "{}",
            "after_json": "{}",
            "evidence_json": canonical_json(evidence),
            "payload_json": canonical_json(audit_payload),
            "audit_hash": audit_hash,
        }
        batch_members[checkpoint_id] = [{
            **row,
            "chain_depth": 1,
            "fact_hash_valid": 1,
            "automatic_real_order_submission": 0,
            "real_order_authority": 0,
        }]
    return {
        "checkpoints": checkpoints,
        "runs": runs,
        "audits": audits,
        "batch_members": batch_members,
        "authoritative_sessions": {
            "test-current-checkpoint": {
                "HISTORY": sorted(row["trade_date"] for row in rows),
                "REPLAY": sorted(row["trade_date"] for row in rows),
            },
        },
    }


def test_tip_addressed_daily_fact_chain_replays_exactly_and_rejects_tamper():
    first, _ = _fact_row(day="2026-08-21", run_uid="1" * 32, depth=2)
    second, _ = _fact_row(
        day="2026-08-24",
        run_uid="2" * 32,
        previous_id=first["fact_id"],
        previous_hash=first["fact_hash"],
        depth=1,
    )
    members = [
        {"fact_id": first["fact_id"], "fact_hash": first["fact_hash"]},
        {"fact_id": second["fact_id"], "fact_hash": second["fact_hash"]},
    ]
    state = {
        "schema": FUNDING_CHECKPOINT_SCHEMA,
        "strategy_key": "alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-24",
        "history_start_date": "2026-08-21",
        "history_end_date": "2026-08-24",
        "history_fact_count": 2,
        "history_opening_equity": "100.00000000",
        "history_opening_date": "2026-08-20",
        "history_tip_fact_id": second["fact_id"],
        "history_tip_fact_hash": second["fact_hash"],
        "history_fact_set_hash": ordered_funding_fact_set_hash(members),
    }
    origin_context = _origin_context([first, second])
    replay = governance._verify_funding_daily_fact_chain(
        checkpoint_row={"checkpoint_id": "test-current-checkpoint"},
        state=state, fact_rows=[first, second],
        origin_context=origin_context,
    )
    assert [row["trade_date"] for row in replay["daily_records"]] == [
        "2026-08-21", "2026-08-24"
    ]
    assert replay["opening_normalized_equity"] == "100.00000000"
    tampered = dict(second)
    tampered["fact_json"] = second["fact_json"].replace(
        '"closing_cash_cny":"1000.000000"',
        '"closing_cash_cny":"999.000000"',
    )
    with pytest.raises(RuntimeError, match="哈希"):
        governance._verify_funding_daily_fact_chain(
            checkpoint_row={"checkpoint_id": "test-current-checkpoint"},
            state=state, fact_rows=[first, tampered],
            origin_context=origin_context,
        )


@pytest.mark.parametrize("actual_cost", ["-0.000000000001", "NaN", "Infinity"])
def test_daily_fact_chain_rejects_invalid_actual_cost(actual_cost):
    row, payload = _fact_row(day="2026-08-24", run_uid="3" * 32, depth=1)
    payload["actual_cost_pct"] = actual_cost
    row["fact_json"] = canonical_json(payload)
    row["fact_hash"] = funding_daily_fact_hash(payload)
    state = {
        "strategy_key": "alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-24",
        "history_start_date": "2026-08-24",
        "history_end_date": "2026-08-24",
        "history_fact_count": 1,
        "history_opening_equity": "100.00000000",
        "history_opening_date": "2026-08-21",
        "history_tip_fact_id": row["fact_id"],
        "history_tip_fact_hash": row["fact_hash"],
        "history_fact_set_hash": ordered_funding_fact_set_hash([row]),
    }
    with pytest.raises(RuntimeError, match="标准化净值或费用"):
        governance._verify_funding_daily_fact_chain(
            checkpoint_row={"checkpoint_id": "test-current-checkpoint"},
            state=state, fact_rows=[row],
            origin_context=_origin_context([row]),
        )


def test_daily_fact_chain_rejects_origin_batch_or_audit_tamper():
    row, _ = _fact_row(day="2026-08-24", run_uid="4" * 32, depth=1)
    state = {
        "strategy_key": "alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-24",
        "history_start_date": "2026-08-24",
        "history_end_date": "2026-08-24",
        "history_fact_count": 1,
        "history_opening_equity": "100.00000000",
        "history_opening_date": "2026-08-21",
        "history_tip_fact_id": row["fact_id"],
        "history_tip_fact_hash": row["fact_hash"],
        "history_fact_set_hash": ordered_funding_fact_set_hash([row]),
    }
    context = _origin_context([row])
    audit_id = next(iter(context["audits"]))
    context["audits"][audit_id]["action"] = "FORGED"
    with pytest.raises(RuntimeError, match="审计锚"):
        governance._verify_funding_daily_fact_chain(
            checkpoint_row={"checkpoint_id": "test-current-checkpoint"},
            state=state, fact_rows=[row],
            origin_context=context,
        )
    context = _origin_context([row])
    checkpoint_id = row["origin_checkpoint_id"]
    context["batch_members"][checkpoint_id][0]["fact_hash_valid"] = 0
    with pytest.raises(RuntimeError, match="批次身份或链"):
        governance._verify_funding_daily_fact_chain(
            checkpoint_row={"checkpoint_id": "test-current-checkpoint"},
            state=state, fact_rows=[row],
            origin_context=context,
        )


def test_checkpoint_anchor_requires_exact_three_roots_canonical_revision_and_cap():
    row, _ = _fact_row(day="2026-08-24", run_uid="5" * 32, depth=1)
    context = _origin_context([row])
    checkpoint = context["checkpoints"][row["origin_checkpoint_id"]]
    run = context["runs"][row["anchor_run_uid"]]
    audit = context["audits"][row["anchor_audit_id"]]

    entry = governance._verify_funding_checkpoint_anchor_contract(
        checkpoint=checkpoint, run=run, audit=audit,
        require_current_canonical=True,
    )
    assert entry["checkpoint_id"] == checkpoint["checkpoint_id"]

    oversized_run = dict(run)
    oversized_run["result_json_bytes"] = (
        governance.FUNDING_CANONICAL_RESULT_MAX_BYTES + 1
    )
    with pytest.raises(RuntimeError):
        governance._verify_funding_checkpoint_anchor_contract(
            checkpoint=checkpoint, run=oversized_run, audit=audit,
            require_current_canonical=True,
        )

    superseded_run = dict(run)
    superseded_run["is_canonical"] = 0
    with pytest.raises(RuntimeError):
        governance._verify_funding_checkpoint_anchor_contract(
            checkpoint=checkpoint, run=superseded_run, audit=audit,
            require_current_canonical=True,
        )

    root_tamper = dict(audit)
    evidence = governance._json(root_tamper["evidence_json"], None)
    evidence["checkpoint_root"] = dict(evidence["checkpoint_root"])
    evidence["checkpoint_root"]["root_hash"] = "f" * 64
    root_tamper["evidence_json"] = canonical_json(evidence)
    with pytest.raises(RuntimeError):
        governance._verify_funding_checkpoint_anchor_contract(
            checkpoint=checkpoint, run=run, audit=root_tamper,
            require_current_canonical=True,
        )


def test_authoritative_sessions_reject_missing_middle_day_even_with_rehash():
    authoritative = {
        "REPLAY": ["2026-08-20", "2026-08-21", "2026-08-24"],
        "HISTORY": ["2026-08-20", "2026-08-21", "2026-08-24"],
    }
    state = {
        "replay_sessions": list(authoritative["REPLAY"]),
        "replay_session_hash": governance._digest({
            "schema": "probiga.strategy-funding-replay-sessions.v1",
            "sessions": authoritative["REPLAY"],
        }),
        "history_start_date": "2026-08-20",
        "history_end_date": "2026-08-24",
        "bootstrap_full_history_scan": False,
    }
    row = {
        "trade_date": "2026-08-24",
        "replay_mode": "BOUNDED_INCREMENTAL",
        "replay_start_date": "2026-08-20",
        "replay_session_count": 3,
        "history_fact_count": 3,
    }
    assert governance._verify_funding_session_contract(
        row=row, state=state, authoritative_sessions=authoritative, key="alpha",
    ) == authoritative["REPLAY"]
    missing_middle = ["2026-08-20", "2026-08-24"]
    forged_state = {
        **state,
        "replay_sessions": missing_middle,
        "replay_session_hash": governance._digest({
            "schema": "probiga.strategy-funding-replay-sessions.v1",
            "sessions": missing_middle,
        }),
    }
    forged_row = {**row, "replay_session_count": 2}
    with pytest.raises(RuntimeError, match="重放会话合同"):
        governance._verify_funding_session_contract(
            row=forged_row,
            state=forged_state,
            authoritative_sessions=authoritative,
            key="alpha",
        )


def test_full_bootstrap_sessions_are_rebuilt_without_inline_history():
    sessions = ["2026-08-20", "2026-08-21", "2026-08-24"]
    state = {
        "replay_sessions": [],
        "replay_session_hash": governance._digest({
            "schema": "probiga.strategy-funding-replay-sessions.v1",
            "sessions": sessions,
        }),
        "history_start_date": "2026-08-20",
        "history_end_date": "2026-08-24",
        "bootstrap_full_history_scan": True,
    }
    row = {
        "trade_date": "2026-08-24",
        "replay_mode": "FULL_BOOTSTRAP",
        "replay_start_date": "2026-08-20",
        "replay_session_count": 3,
        "history_fact_count": 3,
    }
    assert governance._verify_funding_session_contract(
        row=row,
        state=state,
        authoritative_sessions={"REPLAY": sessions, "HISTORY": sessions},
        key="alpha",
    ) == sessions


def test_frozen_schema_contract_hash_is_literal_and_nonempty():
    assert FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH == (
        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
    )
