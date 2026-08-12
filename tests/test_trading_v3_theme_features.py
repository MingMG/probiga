from server.trading_v3.theme_features import (
    diversified_universe_codes,
    infer_name_theme_memberships,
)


def _feature(
    code: str,
    *,
    theme: str = "",
    board: float = 0.0,
    lead: float = 0.0,
    novelty: float = 0.0,
    source: str = "concept",
):
    candidates = []
    if theme:
        candidates.append({
            "theme_code": theme,
            "theme_composite_score": board,
            "stock_leadership_score": lead,
            "theme_news_novelty_score": novelty,
            "theme_source": source,
            "theme_cluster_keys": [
                "LITHIUM" if "LITHIUM" in theme else theme
            ],
        })
    return {
        "stock_code": code,
        "latest_amount": 100_000_000,
        "amount_ratio_5_20": 1.0,
        "amount_ratio_1_20": 1.0,
        "latest_change_pct": 0.0,
        "return_5d_pct": 0.0,
        "return_20d_pct": 0.0,
        "relative_strength_20d_pct": 0.0,
        "distance_ma20_pct": 0.0,
        "drawdown_20d_pct": 0.0,
        "rebound_from_low_pct": 0.0,
        "atr_14d_pct": 2.0,
        "theme_opportunity_score": board,
        "stock_leadership_score": lead,
        "news_theme_context_score": board,
        "theme_signal_candidates": candidates,
    }


def test_name_membership_uses_narrow_taxonomy_markers() -> None:
    memberships = infer_name_theme_memberships({
        "002240": "盛新锂能",
        "603399": "永杉锂业",
        "600000": "浦发银行",
    })

    assert memberships["002240"] == [
        ("NAME_CLUSTER:LITHIUM", "锂产业链", "name_keyword")
    ]
    assert memberships["603399"] == [
        ("NAME_CLUSTER:LITHIUM", "锂产业链", "name_keyword")
    ]
    assert "600000" not in memberships


def test_emerging_theme_leaders_reach_bounded_evaluation_universe() -> None:
    base = {
        f"60{index:04d}": _feature(f"60{index:04d}")
        for index in range(60)
    }
    base["002240"] = _feature(
        "002240", theme="NAME_CLUSTER:LITHIUM", board=0.92, lead=0.88
    )
    base["603399"] = _feature(
        "603399", theme="NAME_CLUSTER:LITHIUM", board=0.92, lead=0.81
    )

    selected = diversified_universe_codes(base, limit=20)

    assert len(selected) == 20
    assert "002240" in selected
    assert "603399" in selected


def test_after_close_novelty_reserves_pullback_theme_for_next_day_review() -> None:
    base = {
        f"60{index:04d}": _feature(f"60{index:04d}")
        for index in range(80)
    }
    base["002240"] = _feature(
        "002240",
        theme="LITHIUM_RAW_MATERIAL",
        board=0.19,
        lead=0.30,
        novelty=0.74,
    )
    base["603399"] = _feature(
        "603399",
        theme="LITHIUM_RAW_MATERIAL",
        board=0.19,
        lead=0.28,
        novelty=0.74,
    )

    selected = diversified_universe_codes(base, limit=20)

    assert "002240" in selected
    assert "603399" in selected


def test_semantic_theme_reserve_keeps_three_leaders_before_other_themes() -> None:
    base = {
        f"60{index:04d}": _feature(
            f"60{index:04d}",
            theme=f"THEME_{index}",
            board=0.95 - index * 0.001,
            lead=0.9,
            novelty=0.9,
        )
        for index in range(40)
    }
    base["002240"] = _feature(
        "002240", theme="LITHIUM_A", board=0.19, lead=0.31,
        novelty=0.74, source="name_keyword",
    )
    base["603399"] = _feature(
        "603399", theme="LITHIUM_B", board=0.19, lead=0.29,
        novelty=0.74, source="name_keyword",
    )
    base["002245"] = _feature(
        "002245", theme="LITHIUM_C", board=0.19, lead=0.30,
        novelty=0.74, source="name_keyword",
    )

    selected = diversified_universe_codes(base, limit=20)

    assert "002240" in selected
    assert "603399" in selected
    assert "002245" in selected
