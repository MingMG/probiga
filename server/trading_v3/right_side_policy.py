from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from .calibration import CALIBRATION_PROTOCOL


RETURN_20D_RANGE = (2.0, 22.0)
RETURN_60D_RANGE = (12.0, 55.0)
MA20_SLOPE_5D_RANGE = (0.2, 4.0)
DISTANCE_MA20_RANGE = (0.0, 8.0)
AMOUNT_RATIO_5_20_RANGE = (0.9, 1.8)
MAXIMUM_LATEST_CHANGE_PCT = 9.5
MINIMUM_MARKET_RETURN_20D_PCT = 2.0
MINIMUM_AVERAGE_AMOUNT_20D = 50_000_000.0
MINIMUM_PRICE = 2.0
RIGHT_SIDE_LABEL_PROTOCOL = "DYNAMIC_EXIT_CENSORED_CANDIDATES_V2"
RIGHT_SIDE_FEATURE_COLUMNS = (
    "return_2d_pct",
    "return_5d_pct",
    "return_20d_pct",
    "return_60d_pct",
    "ma20_slope_5d_pct",
    "breakout_20d_proximity",
    "amount_ratio_5_20",
    "relative_strength_20d_pct",
    "market_latest_change_pct",
    "latest_relative_to_market_pct",
    "distance_ma20_pct",
    "distance_ma5_pct",
    "drawdown_20d_pct",
    "rebound_from_low_pct",
    "previous_change_pct",
    "amount_ratio_1_20",
    "close_above_ma20",
    "ma20_above_ma60",
    "atr_14d_pct",
)
RIGHT_SIDE_SCORE_SPEC = {
    "formula": "right_side_trend_v302_score_v1",
    "weights": {
        "trend_alignment": 0.24,
        "return_20d": 0.16,
        "return_60d": 0.14,
        "ma20_slope_5d": 0.15,
        "relative_strength_20d": 0.12,
        "amount_ratio_5_20": 0.08,
        "distance_ma20_centered_4": 0.07,
        "inverse_atr_14d": 0.04,
    },
}


def _number(features: Mapping[str, Any], key: str) -> float:
    return float(features.get(key) or 0.0)


def right_side_setup_ready(features: Mapping[str, Any]) -> bool:
    """Return whether a live signal belongs to the calibrated universe."""

    return bool(
        _number(features, "entry_eligible") >= 1.0
        and _number(features, "latest_tradable") >= 1.0
        and _number(features, "price") >= MINIMUM_PRICE
        and _number(features, "latest_amount") > 0.0
        and _number(features, "average_amount_20d")
        >= MINIMUM_AVERAGE_AMOUNT_20D
        and _number(features, "close_above_ma20") >= 1.0
        and _number(features, "ma20_above_ma60") >= 1.0
        and RETURN_20D_RANGE[0]
        <= _number(features, "return_20d_pct")
        <= RETURN_20D_RANGE[1]
        and RETURN_60D_RANGE[0]
        <= _number(features, "return_60d_pct")
        <= RETURN_60D_RANGE[1]
        and MA20_SLOPE_5D_RANGE[0]
        <= _number(features, "ma20_slope_5d_pct")
        <= MA20_SLOPE_5D_RANGE[1]
        and DISTANCE_MA20_RANGE[0]
        <= _number(features, "distance_ma20_pct")
        <= DISTANCE_MA20_RANGE[1]
        and AMOUNT_RATIO_5_20_RANGE[0]
        <= _number(features, "amount_ratio_5_20")
        <= AMOUNT_RATIO_5_20_RANGE[1]
        and _number(features, "latest_change_pct")
        < MAXIMUM_LATEST_CHANGE_PCT
    )


def right_side_model_contract_hash(
    config: Mapping[str, Any],
) -> str:
    """Bind a calibration to formula, universe, ranking, and labels."""

    calibration = dict(config.get("calibration") or {})
    payload = {
        "feature_columns": RIGHT_SIDE_FEATURE_COLUMNS,
        "score": RIGHT_SIDE_SCORE_SPEC,
        "setup": {
            "return_20d_range": RETURN_20D_RANGE,
            "return_60d_range": RETURN_60D_RANGE,
            "ma20_slope_5d_range": MA20_SLOPE_5D_RANGE,
            "distance_ma20_range": DISTANCE_MA20_RANGE,
            "amount_ratio_5_20_range": AMOUNT_RATIO_5_20_RANGE,
            "maximum_latest_change_pct": MAXIMUM_LATEST_CHANGE_PCT,
            "minimum_market_return_20d_pct": (
                MINIMUM_MARKET_RETURN_20D_PCT
            ),
            "minimum_average_amount_20d": MINIMUM_AVERAGE_AMOUNT_20D,
            "minimum_price": MINIMUM_PRICE,
        },
        "top_per_day": int(calibration.get("top_per_day", 10)),
        "bucket_count": int(calibration.get("bucket_count", 5)),
        "calibration_protocol": CALIBRATION_PROTOCOL,
        "minimum_bucket_count": int(
            calibration.get("minimum_bucket_count", 3)
        ),
        "label_protocol": RIGHT_SIDE_LABEL_PROTOCOL,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
