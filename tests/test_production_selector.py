from server.engine.production_selector import (
    MODEL_FINGERPRINT,
    board_limit_pct,
    board_limit_trigger_pct,
    rank_production_candidates,
    score_production_candidate,
    selector_contract,
)


def _full_row(code: str = "600001", base: float = 70) -> dict:
    return {
        "stock_code": code,
        "final_trade_score": base,
        "entry_score": 82,
        "quality_score": 76,
        "risk_reward_ratio": 3.2,
        "heat_overload_score": 20,
        "failure_penalty_score": 10,
        "event_risk_level": "LOW",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": 1,
        "volume": 10_000_000,
        "amount": 100_000_000,
        "correlation_cluster": f"CORR:{code}",
        "correlation_cluster_status": "VERIFIED_60D",
        "market_mood_score": 65,
        "global_market_regime_score": 65,
        "sentiment_score": 62,
        "ultra_short_score": 72,
        "main_wave_score": 78,
        "capital_score": 75,
        "sector_rotation_score": 68,
        "fundamental": 80,
        "valuation": 70,
        "long_term_score": 74,
        "expected_return_score": 72,
        "finance_pit_verified": True,
        "data_date": "2026-08-10",
        "finance_report_date": "2026-06-30",
        "finance_notice_date": "2026-07-20",
        "finance_knowledge_at": "2026-07-20 08:00:00",
    }


def test_all_four_versions_actually_contribute_to_production_ranking():
    result = score_production_candidate(_full_row())

    assert result["active_versions"] == ["V3", "V4", "V5", "V6"]
    assert result["selector_versions"]["V4"]["status"] == "ACTIVE_BOUNDED"
    assert result["selector_versions"]["V5"]["details"]["regime"] == "RISK_ON"
    assert result["selector_versions"]["V6"]["details"]["pit_verified"] is True
    assert result["ensemble_score"] != result["base_score"]
    assert set(result["multi_horizon"]["scores"]) == {"T+1", "T+5", "T+20"}
    assert result["execution_diagnostics"]["status"] == "PASS"
    assert result["order_authority"] is False


def test_missing_version_evidence_transfers_authority_back_to_v3():
    result = score_production_candidate({"stock_code": "000001", "final_trade_score": 81})

    assert result["score"] == 81
    assert result["active_versions"] == ["V3"]
    for version in ("V4", "V5", "V6"):
        assert result["selector_versions"][version]["status"] == "FALLBACK_TO_V3"
        assert result["selector_contributions"][version] == 0


def test_advisory_versions_can_rerank_but_cannot_overwrite_order_fields():
    stronger_base = _full_row("600001", 72)
    stronger_base.update(
        {
            "entry_score": 10,
            "quality_score": 10,
            "risk_reward_ratio": 0.5,
            "event_risk_level": "HIGH",
            "chase_risk_status": "EXECUTION_BLOCKED",
            "market_mood_score": 35,
            "sentiment_score": 30,
            "long_term_score": 10,
            "swing_score": 10,
            "fundamental": 10,
            "valuation": 10,
            "expected_return_score": 10,
            "action": "DATA_BLOCKED",
            "actionable": False,
        }
    )
    slightly_weaker_base = _full_row("600002", 70)
    slightly_weaker_base.update({"action": "WATCH", "actionable": False})

    ranked = rank_production_candidates([stronger_base, slightly_weaker_base])

    assert [row["stock_code"] for row in ranked] == ["600002", "600001"]
    assert all(row["actionable"] is False for row in ranked)
    assert ranked[1]["action"] == "DATA_BLOCKED"


def test_contract_caps_advisory_authority_and_never_allows_orders():
    contract = selector_contract()

    assert contract["max_total_advisory_weight"] == 0.30
    assert contract["missing_data_policy"] == "FAIL_CLOSED_TRANSFER_SOFT_WEIGHT_TO_V3"
    assert contract["risk_gate_policy"] == "V4_HARD_VETO_CANNOT_BE_OVERRIDDEN"
    assert contract["finance_policy"] == "V6_POINT_IN_TIME_REQUIRED"
    assert contract["model_fingerprint"] == MODEL_FINGERPRINT
    assert contract["production_ranking_active"] is True
    assert contract["order_authority"] is False
    assert contract["automatic_real_order_submission"] is False


def test_board_limit_rules_cover_main_growth_star_bse_and_st():
    assert board_limit_pct("600001") == 10.0
    assert board_limit_pct("300001") == 20.0
    assert board_limit_pct("688001") == 20.0
    assert board_limit_pct("920001") == 30.0
    assert board_limit_pct("600001", "*ST 测试") == 5.0
    assert board_limit_trigger_pct("300001") == 19.0


def test_v4_hard_reject_cannot_be_overridden_by_high_other_scores():
    row = _full_row(base=99)
    row.update({"event_risk_level": "CRITICAL", "limit_up_locked": True})

    result = score_production_candidate(row)

    assert result["selector_versions"]["V4"]["status"] == "HARD_REJECT"
    assert result["risk_gate"]["hard_veto"] is True
    assert result["candidate_grade"] == "REJECT"
    assert result["order_authority"] is False


def test_v5_uses_global_regime_instead_of_stock_sentiment():
    row = _full_row()
    row.update({"global_market_regime_score": 30, "market_mood_score": 95, "sentiment_score": 99})

    result = score_production_candidate(row)

    assert result["selector_versions"]["V5"]["details"]["regime"] == "RISK_OFF"
    assert result["selector_versions"]["V5"]["details"]["global_regime_score"] == 30


def test_v6_rejects_future_finance_knowledge_and_falls_back():
    row = _full_row()
    row["finance_knowledge_at"] = "2026-08-11 01:00:00"

    result = score_production_candidate(row)

    assert result["selector_versions"]["V6"]["status"] == "FALLBACK_TO_V3"
    assert "future_knowledge_date" in result["selector_versions"]["V6"]["details"]["pit_failures"]


def test_cross_section_normalizes_finance_and_applies_portfolio_concentration():
    rows = []
    for index in range(4):
        row = _full_row(f"60000{index + 1}", 84 - index)
        row.update(
            {
                "industry_name": "同一行业",
                "primary_concept": "同一主题",
                "correlation_cluster": f"cluster-{index}",
                "roe_wtd": 8 + index,
                "gross_margin": 20 + index,
                "net_margin": 5 + index,
                "asset_liab_ratio": 40 - index,
                "oper_cf_ps": 1 + index,
                "cash_flow_ratio": 10 + index,
                "net_profit_yoy_gr": 5 + index,
                "net_asset_ps": 5 + index,
                "close": 10,
            }
        )
        rows.append(row)

    ranked = rank_production_candidates(rows)

    assert ranked[0]["normalization"]["peer_count"] == 4
    assert ranked[0]["selector_versions"]["V6"]["status"] == "ACTIVE_BOUNDED"
    assert sum(bool(row["portfolio_eligible"]) for row in ranked) == 3
    assert "industry_concentration" in ranked[3]["portfolio_reject_reasons"]
