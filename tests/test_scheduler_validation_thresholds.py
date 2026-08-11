# -*- coding: utf-8 -*-

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS


def test_realtime_quote_threshold_matches_valid_market_universe():
    assert TASK_OUTPUT_REQUIREMENTS["stock_current"][0].min_distinct == 5000
    assert TASK_OUTPUT_REQUIREMENTS["intraday_realtime"][0].min_distinct == 5000
