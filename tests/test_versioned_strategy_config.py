from server.common.versioned_strategy_config import (
    legacy_strategy_merge_map,
    load_market_state_config,
    load_stock_manifest,
    market_state_config_hash,
    stock_manifest_hash,
    stock_strategy_catalog,
    stock_strategy_profiles,
)


def test_stock_manifest_is_frozen_and_has_only_real_stock_modes():
    manifest = load_stock_manifest()
    assert manifest["status"] == "frozen"
    assert [item["key"] for item in manifest["strategies"]] == [
        "ultra_short",
        "short_term",
        "swing",
        "main_wave",
    ]
    assert len(stock_manifest_hash()) == 64
    assert len(stock_strategy_catalog()) == 4


def test_stock_profiles_are_loaded_from_frozen_manifest():
    profiles = stock_strategy_profiles()
    assert profiles["ultra_short"]["confirm_score"] == 76.0
    assert profiles["short_term"]["stop_loss_pct"] == -5.5
    assert profiles["swing"]["max_holding_days"] == 30
    assert profiles["main_wave"]["extension_days_when_trend_valid"] == 30


def test_fake_or_cross_asset_strategy_labels_are_merged_or_disabled():
    aliases = legacy_strategy_merge_map()
    assert aliases["multi_factor"] == "short_term"
    assert aliases["trend_breakout"] == "main_wave"
    assert aliases["machine_learning"] is None
    assert aliases["etf_defense"] is None


def test_market_state_config_is_frozen_and_complete():
    config = load_market_state_config()
    assert config["status"] == "frozen_baseline"
    assert config["thresholds"]["trend_bullish"]["missing_breadth_policy"] == "block"
    assert config["transition"]["cooldown_after_extreme_trade_days"] == 3
    assert len(market_state_config_hash()) == 64
