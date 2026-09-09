from biz.review.selection_review import build_selection_review, render_selection_review
from server.common.daily_delivery_control import canonical_sha256


def _review(**kwargs):
    values = dict(execution_date="2026-09-09", plans=[], intents=[], fills=[],
                  bars=[], forward=[], health=[], frozen_universe=[],
                  source_roots={"plan": "a" * 64}, issues=[])
    values.update(kwargs)
    return build_selection_review(**values)


def _plan(code="000001", version="v1"):
    return dict(stock_code=code, strategy_key="trend", strategy_version=version,
                source_run_uid="r", plan_hash="a" * 64)


def _intent(code="000001", intent_id="i1"):
    return dict(stock_code=code, intent_id=intent_id, strategy_version="v1",
                evidence_json={"primary_strategy_key": "trend", "primary_strategy_version": "v1",
                    "strategy_governance": {"governance_run_uid": "r", "target_hash": "a" * 64,
                                            "strategy_key": "trend", "strategy_version": "v1"}},
                waiting_reason="LIMIT_NOT_REACHED")


def test_review_keeps_filled_unfilled_and_unmaterialized_plans_separate():
    review = _review(plans=[_plan(), _plan("000002"), _plan("000003")],
        intents=[_intent(), _intent("000002", "i2")],
        fills=[dict(fill_id="f1", intent_id="i1", side="BUY", quantity=100,
                    gross_amount=1000, fee_amount=5)],
        bars=[dict(stock_code="000001", open=9, close=11)])
    assert review["execution_counts"] == {"FILLED": 1, "UNFILLED": 1, "NOT_MATERIALIZED": 1}
    assert review["stocks"][0]["entry_to_close_mark_pct"] == 10
    assert review["stocks"][0]["entry_fee_cny"] == 5
    assert review["strategies"][0]["windows_calendar_days"]["20"]["matured_count"] == 0
    assert "NOT_REALIZED" in review["stocks"][0]["mark_semantics"]
    assert review["strategy_changes_applied_by_review"] is False


def test_review_does_not_mix_versions_open_evidence_or_future_exits():
    base = dict(strategy_key="trend", strategy_version="v1", evidence_status="MATURED",
                sample_owner_role="PRIMARY", attribution_status="VERIFIED_SNAPSHOT",
                exit_at="2026-09-09 14:00:00", realized_net_return_pct=-2)
    forward = [dict(base, evidence_id=str(i)) for i in range(20)] + [
        dict(base, strategy_version="v2", realized_net_return_pct=80),
        dict(base, evidence_status="OPEN", realized_net_return_pct=80),
        dict(base, exit_at="2026-09-10 14:00:00", realized_net_return_pct=80),
        dict(base, sample_owner_role="SUPPORTING", realized_net_return_pct=80),
        dict(base, attribution_status="LEGACY_VERSION_DERIVED", realized_net_return_pct=80),
    ]
    review = _review(plans=[_plan()], forward=forward)
    strategy = review["strategies"][0]
    assert strategy["windows_calendar_days"]["20"]["matured_count"] == 20
    assert strategy["windows_calendar_days"]["20"]["net_expectancy_pct"] == -2
    assert strategy["review_action"] == "REVIEW_WEAK_STRATEGY"


def test_missed_move_diagnostics_use_entire_frozen_universe_not_only_winners():
    review = _review(plans=[_plan()], frozen_universe=[dict(stock_code=f"00000{i}") for i in (1, 2, 3)],
        bars=[dict(stock_code="000002", open=10, close=12),
              dict(stock_code="000003", open=10, close=9),
              dict(stock_code="000004", open=10, close=15)])
    assert review["unselected_measured_count"] == 2
    assert [row["stock_code"] for row in review["missed_move_diagnostics"]] == ["000002", "000003"]
    assert all(row["executable_opportunity_proven"] is False for row in review["missed_move_diagnostics"])
    assert review["review_sha256"] == canonical_sha256({key: value for key, value in review.items() if key != "review_sha256"})


def test_no_plan_is_visible_diagnosis_not_zero_return_success():
    review = _review(source_roots={}, issues=["ANALYSIS_PUBLISHED_SNAPSHOT_UNAVAILABLE"])
    assert review["status"] == "DEGRADED"
    assert review["strategies"] == []
    assert "不能据此评价" in render_selection_review(review)


def test_another_run_for_same_stock_and_version_is_not_the_frozen_plan():
    intent = _intent()
    intent["evidence_json"]["strategy_governance"]["governance_run_uid"] = "later-run"
    review = _review(plans=[_plan()], intents=[intent],
        fills=[dict(fill_id="f1", intent_id="i1", side="BUY", quantity=100, gross_amount=1000, fee_amount=5)])
    assert review["stocks"][0]["execution_status"] == "NOT_MATERIALIZED"
    assert review["unattributed_intent_ids"] == ["i1"]


def test_partial_mark_coverage_does_not_publish_a_partial_average():
    review = _review(plans=[_plan(), _plan("000002")], intents=[_intent(), _intent("000002", "i2")],
        fills=[dict(fill_id="f" + str(i), intent_id="i" + str(i), side="BUY", quantity=100,
                    gross_amount=1000, fee_amount=5) for i in (1, 2)],
        bars=[dict(stock_code="000001", open=10, close=11)])
    assert review["status"] == "DEGRADED"
    assert review["missing_mark_stock_codes"] == ["000002"]
    assert review["strategies"][0]["mark_coverage_count"] == 1
    assert review["strategies"][0]["average_entry_to_close_mark_pct"] is None
