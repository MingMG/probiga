from __future__ import annotations

import math
from typing import Any


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def daily_exit_reason(
    *,
    protective_stop: Any,
    session_low: Any,
    close_above_ma20: Any,
    ma20_above_ma60: Any,
    hypothesis_invalidated: bool = False,
    require_trend_alignment: bool = True,
) -> str | None:
    """Return the canonical close-decision exit reason for Trading V3.

    A stop touch is observed from a completed daily bar.  The decision is an
    exit intent for the next executable session, not an intraday stop fill.
    """

    stop = _finite_float(protective_stop)
    low = _finite_float(session_low)
    if stop is not None and stop > 0 and low is not None and low <= stop:
        return "HARD_STOP"
    if hypothesis_invalidated:
        return "HYPOTHESIS_INVALIDATED"
    # Left-side discovery sleeves deliberately enter before a full MA20/MA60
    # trend exists.  Their active signal lifecycle and hard stop are the
    # invalidation rules; applying the right-side trend rule would force an
    # immediate next-day exit and make forward discovery evidence meaningless.
    if not require_trend_alignment:
        return None
    close_flag = _finite_float(close_above_ma20)
    alignment_flag = _finite_float(ma20_above_ma60)
    if not (
        close_flag is not None
        and close_flag >= 1.0
        and alignment_flag is not None
        and alignment_flag >= 1.0
    ):
        return "TREND_INVALIDATED"
    return None
