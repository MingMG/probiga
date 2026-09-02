"""Immutable scheduler contract for final canonical-pool WeCom delivery."""
from __future__ import annotations

from server.common.release_data_readiness_contract import (
    FINAL_POOL_WECOM_DELIVERY_TASK_TYPE,
)


TASK = {
    "task_name": "最终策略票池企微交付",
    "task_type": FINAL_POOL_WECOM_DELIVERY_TASK_TYPE,
    "group_name": "资讯公告",
    "script_path": "tools/send_final_pool_wecom.py",
    "script_args": "--json",
    "cron_time": "22:40",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 219,
    "date_param": "",
    "description": (
        "仅在最近两个目标交易日均有精确终态凭证后，通过早报机器人"
        "幂等交付完整最终票池；绑定治理/分析运行、构建、池哈希和竞价门禁，"
        "真实交易始终关闭。"
    ),
}


__all__ = ["TASK"]
