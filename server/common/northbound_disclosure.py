"""Trading dates affected by the Stock Connect disclosure changes."""

from datetime import time

# SSE 上证发〔2024〕106号 and SZSE 深证上〔2024〕601号 took effect
# on 2024-08-19. Northbound daily turnover remains a separate public metric.
NORTHBOUND_DAILY_NET_DISCLOSURE_END = "2024-08-19"
# The first phase stopped real-time Northbound turnover disclosure on 2024-05-13.
NORTHBOUND_INTRADAY_NET_DISCLOSURE_END = "2024-05-13"
NORTHBOUND_NET_NOT_DISCLOSED = "NORTHBOUND_NET_FLOW_NOT_DISCLOSED"
NORTHBOUND_CLOSE_READY_TIME = time(15, 30)
NORTHBOUND_DAILY_RESULT_SCHEMA = "probiga.north-flow-daily-result.v1"
