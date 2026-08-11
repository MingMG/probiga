from tools.backtest_sector_preheat import _candidate_theme_names


def test_candidate_theme_names_include_primary_and_all_aliases():
    candidate = {
        "theme_name": "医药电商",
        "theme_matches": [
            {"theme_name": "创新药"},
            {"theme_name": "创新药"},
            {"theme_name": "抗肿瘤"},
        ],
    }

    assert _candidate_theme_names(candidate) == [
        "创新药",
        "医药电商",
        "抗肿瘤",
    ]
