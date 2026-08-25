import json
from datetime import date, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import strategy_center as router
from server.engine import strategy_governance as governance


RUN_UID = "a" * 32
RESULT_HASH = "b" * 64
TRADE_DATE = "2026-08-24"


def _verified_checkpoint_source(
    strategy_key: str, checkpoint_digit: str,
) -> dict:
    start = date(2026, 7, 27)
    days = [(start + timedelta(days=index)).isoformat() for index in range(29)]
    days = days[-20:]
    daily = []
    equity = []
    value = 100.0
    for index, day in enumerate(days):
        daily_return = 0.2 if index == 0 else 0.1
        value *= 1 + daily_return / 100.0
        daily.append({
            "trade_date": day,
            "return_pct": daily_return,
            "actual_cost_pct": 0.01,
            "is_net_return": True,
            "evidence_revision_at": f"{day}T15:00:00",
        })
        equity.append({"trade_date": day, "equity": value})
    fact_members = [{
        "fact_id": f"{index + 1:064x}",
        "fact_hash": f"{index + 1000:064x}",
        "trade_date": day,
    } for index, day in enumerate(days)]
    state = {
        "schema": governance.FUNDING_CHECKPOINT_SCHEMA,
        "strategy_key": strategy_key,
        "strategy_version": "v1",
        "account_id": "paper-main-v2",
        "trade_date": days[-1],
        "replay_mode": "BOUNDED_INCREMENTAL",
        "replay_session_count": 1,
        "max_holding_days": 20,
        "history_start_date": days[0],
        "history_end_date": days[-1],
        "history_fact_count": len(days),
        "history_fact_set_hash": governance.ordered_funding_fact_set_hash(
            fact_members,
        ),
        "holdings": [],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    checkpoint_id = checkpoint_digit * 64
    checkpoint_hash = governance.checkpoint_state_hash(state)
    verified_checkpoint = {
        "state": state,
        "checkpoint_id": checkpoint_id,
        "checkpoint_hash": checkpoint_hash,
        "chain_hash": ("c" if checkpoint_digit != "c" else "d") * 64,
    }
    verified_fact_chain = {
        "daily_records": daily,
        "equity_curve": equity,
        "daily_stock_market_values": [{
            "trade_date": day,
            "stock_closing_market_values": {},
            "stock_intraday_turnover_proxy": {},
            "stock_risk_exposure": {},
        } for day in days],
        "closed_evidence_by_day": [{
            "trade_date": day, "evidence_ids": [],
        } for day in days],
        "fact_members": fact_members,
    }
    return {
        "schema": governance.FUNDING_CHECKPOINT_DETAIL_SOURCE_SCHEMA,
        "checkpoint_id": checkpoint_id,
        "strategy_key": strategy_key,
        "strategy_version": "v1",
        "account_id": "paper-main-v2",
        "trade_date": days[-1],
        "anchor_run_uid": RUN_UID,
        "anchor_current_canonical": True,
        "verified_checkpoint": verified_checkpoint,
        "verified_fact_chain": verified_fact_chain,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _strategy_ref(source: dict) -> dict:
    state = source["verified_checkpoint"]["state"]
    return {
        "checkpoint_id": source["checkpoint_id"],
        "strategy_key": source["strategy_key"],
        "strategy_version": source["strategy_version"],
        "account_id": source["account_id"],
        "trade_date": source["trade_date"],
        "checkpoint_hash": source["verified_checkpoint"]["checkpoint_hash"],
        "chain_hash": source["verified_checkpoint"]["chain_hash"],
        "history_fact_set_hash": state["history_fact_set_hash"],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _combination_source(left: dict, right: dict) -> dict:
    binding_payload = {
        "schema": "probiga.combination-drift-risk-binding.v2",
        "window_days": 60,
        "risk_path_hash": "1" * 64,
        "constraint_evaluation_hash": "2" * 64,
        "constraint_passed": True,
        "peak_member_weight": 0.6,
        "current_member_weight": 0.6,
        "peak_pairwise_stock_overlap_pct": 0.0,
        "current_pairwise_stock_overlap_pct": 0.0,
        "peak_industry_weight_pct": 60.0,
        "current_industry_weight_pct": 60.0,
        "industry_snapshot_path_hash": "3" * 64,
        "industry_trade_dates_hash": "4" * 64,
        "industry_stock_code_sets_hash": "5" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {
        "schema": governance.FUNDING_COMBINATION_RECIPE_DETAIL_SOURCE_SCHEMA,
        "run_uid": RUN_UID,
        "combination_key": "combo_alpha",
        "combination_version": "v1",
        "trade_date": left["trade_date"],
        "recipe_hash": "e" * 64,
        "recipe_gate_hash": "f" * 64,
        "risk_constraint_binding": {
            **binding_payload,
            "binding_hash": governance._digest(binding_payload),
        },
        "recipe_entry": {},
        "member_count": 2,
        "member_sources": [
            {
                "strategy_key": left["strategy_key"],
                "strategy_version": "v1",
                "weight": 0.6,
                "checkpoint_id": left["checkpoint_id"],
                "source": left,
            },
            {
                "strategy_key": right["strategy_key"],
                "strategy_version": "v1",
                "weight": 0.4,
                "checkpoint_id": right["checkpoint_id"],
                "source": right,
            },
        ],
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def test_funding_detail_page_byte_boxes_and_advances_by_actual_rows():
    source = _verified_checkpoint_source("alpha", "1")
    for item in source["verified_fact_chain"]["daily_records"]:
        item["large_verified_field"] = "x" * (1200 * 1024)

    first = governance._funding_checkpoint_detail_page(
        source["verified_checkpoint"], source["verified_fact_chain"],
        series="daily_records", window_days=20, cursor="", limit=20,
    )
    second = governance._funding_checkpoint_detail_page(
        source["verified_checkpoint"], source["verified_fact_chain"],
        series="daily_records", window_days=20,
        cursor=first["next_cursor"], limit=20,
    )

    assert 1 <= first["row_count"] < 20
    assert second["offset"] == first["row_count"]
    assert len(governance._json_text(first).encode("utf-8")) <= (
        governance.FUNDING_DETAIL_PAGE_MAX_BYTES
    )


def test_funding_detail_rejects_one_oversized_item_without_truncating_it():
    source = _verified_checkpoint_source("alpha", "1")
    source["verified_fact_chain"]["closed_evidence_by_day"][0][
        "oversized_verified_field"
    ] = "x" * (4 * 1024 * 1024)

    with pytest.raises(governance.FundingDetailItemTooLarge):
        governance._funding_checkpoint_detail_page(
            source["verified_checkpoint"], source["verified_fact_chain"],
            series="closed_evidence_by_day", window_days=20,
            cursor="", limit=1,
        )


def test_combination_detail_rebuilds_frozen_member_fact_sets_without_cash():
    left = _verified_checkpoint_source("left_alpha", "1")
    right = _verified_checkpoint_source("right_alpha", "2")
    source = _combination_source(left, right)

    page = governance._combination_recipe_detail_page(
        source, series="daily_records", window_days=20,
        cursor="", limit=20,
    )

    assert page["row_count"] == 20
    assert len(page["member_fact_sets"]) == 2
    assert page["cash_fact_materialized"] is False
    assert page["independent_combination_cash_fact"] is False
    assert page["detail_funding_authority"] is False
    assert page["allocation_semantics"] == (
        "WINDOW_OPEN_REBASED_FIXED_SLEEVES_NATURAL_WEIGHT_DRIFT_V3"
    )
    assert page["items"][0]["return_pct"] == pytest.approx(0.2)
    assert page["automatic_real_order_submission"] is False
    assert page["real_order_authority"] is False


def test_strategy_detail_route_uses_only_canonical_row_ref(monkeypatch):
    source = _verified_checkpoint_source("alpha", "1")
    ref = _strategy_ref(source)
    captured = []
    monkeypatch.setattr(router, "load_canonical_governance_snapshot", lambda **_kwargs: {
        "run_uid": RUN_UID,
        "canonical_result_hash": RESULT_HASH,
        "trade_date": source["trade_date"],
        "statistical_funding_eligible": True,
        "strategies": [{
            "strategy_key": "alpha",
            "current_version": "v1",
            "funding_checkpoint_ref": ref,
        }],
        "combinations": [],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    })

    def load(observed_ref, **_kwargs):
        captured.append(observed_ref)
        return source

    monkeypatch.setattr(
        router, "load_verified_funding_checkpoint_detail_source", load,
    )
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    response = TestClient(app).get(
        "/api/strategy-center/governance/funding/strategies/alpha",
        params={
            "trade_date": source["trade_date"],
            "run_uid": RUN_UID,
            "canonical_result_hash": RESULT_HASH,
            "checkpoint_id": "9" * 64,
            "window_days": 20,
        },
    )

    assert response.status_code == 200
    assert captured == [ref]
    assert response.json()["page"]["checkpoint_id"] == ref["checkpoint_id"]
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False
    assert len(response.content) < 4 * 1024 * 1024


def test_combination_detail_route_requires_canonical_funded_recipe(monkeypatch):
    left = _verified_checkpoint_source("left_alpha", "1")
    right = _verified_checkpoint_source("right_alpha", "2")
    source = _combination_source(left, right)
    recipe_ref = {
        "schema": "probiga.combination-member-fact-recipe.v1",
        "combination_key": "combo_alpha",
        "combination_version": "v1",
        "recipe_hash": source["recipe_hash"],
        "recipe_gate_hash": source["recipe_gate_hash"],
        "risk_constraint_binding": source["risk_constraint_binding"],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    canonical = {
        "run_uid": RUN_UID,
        "canonical_result_hash": RESULT_HASH,
        "trade_date": source["trade_date"],
        "statistical_funding_eligible": True,
        "strategies": [],
        "combinations": [{
            "combination_key": "combo_alpha",
            "current_version": "v1",
            "paper_allocation_eligible": True,
            "funding_recipe_ready": True,
            "combination_recipe_ref": recipe_ref,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    monkeypatch.setattr(
        router, "load_canonical_governance_snapshot", lambda **_kwargs: canonical,
    )
    monkeypatch.setattr(
        router, "load_verified_combination_recipe_detail_source",
        lambda **_kwargs: source,
    )
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    client = TestClient(app)
    params = {
        "trade_date": source["trade_date"],
        "run_uid": RUN_UID,
        "canonical_result_hash": RESULT_HASH,
        "window_days": 20,
    }

    response = client.get(
        "/api/strategy-center/governance/funding/combinations/combo_alpha",
        params=params,
    )
    assert response.status_code == 200
    assert response.json()["page"]["cash_fact_materialized"] is False
    assert response.json()["page"]["detail_funding_authority"] is False

    canonical["combinations"][0]["paper_allocation_eligible"] = False
    blocked = client.get(
        "/api/strategy-center/governance/funding/combinations/combo_alpha",
        params=params,
    )
    assert blocked.status_code == 404
    assert blocked.json()["error"] == (
        "combination_funding_detail_not_available"
    )


def test_funding_detail_ui_calls_real_routes_and_states_recipe_truth():
    source = open("server/static/js/app.js", encoding="utf-8").read()

    assert "window._strategyFundingDetail" in source
    assert "governance/funding/' + endpointType" in source
    assert "只读取当前canonical修订" in source
    assert "不生成独立组合现金事实" in source
    assert "响应硬上限4MiB" in source
