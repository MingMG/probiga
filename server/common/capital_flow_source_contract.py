"""Persisted daily flows retain their provider for every stock/date observation.

The common main bucket is large + superlarge, in CNY. Provider estimates are
not interchangeable measurements: provenance remains part of row hashes and
receipts, including when a partition contains observations from several sources.
"""

EASTMONEY_DAILY_FLOW_SOURCES = frozenset({
    "east", "east_min_close", "east_push2delay", "push2his", "push2hist",
})
PUBLIC_DAILY_FLOW_SOURCES = EASTMONEY_DAILY_FLOW_SOURCES | {"baidu", "sina_l1"}
DAILY_FLOW_HISTORICAL_SOURCES = PUBLIC_DAILY_FLOW_SOURCES | {"gj_big_qmt_inner"}

# Stable evidence labels, not aliases that rewrite the stored source identity.
FLOW_SOURCE_SEMANTICS = {
    **{source: "eastmoney_order_size_net_cny" for source in EASTMONEY_DAILY_FLOW_SOURCES},
    "baidu": "baidu_order_size_net_cny",
    "sina_l1": "sina_l1_trade_size_net_cny_main_r0_plus_r1",
    "gj_big_qmt_inner": "qmt_transactioncount_order_size_net_cny",
}
