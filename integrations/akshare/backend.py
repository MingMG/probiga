# -*- coding: utf-8 -*-
"""AkShare 数据源后端，包装现有 stock_kline_akshare 模块。

仅支持日K线获取。不修改 stock_kline_akshare.py，只在其上层提供统一接口。
"""
from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class AkShareBackend:
    """AkShare 数据源后端。仅支持日K线。"""

    @property
    def name(self) -> str:
        return "akshare"

    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取日K线，返回 sm_stock_kline schema。"""
        from biz.stock_market.stock_kline_akshare import (
            ADJUST_TO_INT,
            _to_yyyymmdd,
            akshare_daily_to_sm_kline,
            fetch_stock_daily_kline,
        )

        adjust = kwargs.get("adjust", "") or os.environ.get(
            "SM_STOCK_KLINE_AKSHARE_ADJUST", ""
        )
        adjust_type = ADJUST_TO_INT.get(adjust, 0)
        short_name_map = kwargs.get("short_name_map", {})

        start_api = _to_yyyymmdd(start_date)
        end_api = _to_yyyymmdd(end_date)

        parts = []
        for code in stock_codes:
            try:
                raw = fetch_stock_daily_kline(code, start_api, end_api, adjust)
                if raw is not None and not raw.empty:
                    sm_df = akshare_daily_to_sm_kline(
                        raw,
                        code,
                        1,
                        adjust_type,
                        short_name=short_name_map.get(code, ""),
                    )
                    if not sm_df.empty:
                        parts.append(sm_df)
            except Exception as exc:
                logger.warning("AkShare K线获取失败 %s: %s", code, exc)

        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError("AkShare 不支持分钟数据")

    def fetch_current(
        self,
        stock_codes: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError("AkShare 不支持实时行情")


# ---------------------------------------------------------------------------
# 注册
# ---------------------------------------------------------------------------
from integrations.registry import register  # noqa: E402

register("akshare", lambda: AkShareBackend())
