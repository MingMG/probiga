# -*- coding: utf-8 -*-
"""数据源后端统一协议定义。

所有数据源后端（adata、MyQuant、QMT 等）只需实现此 Protocol 的方法签名，
无需继承基类。后端只负责取数据 + 转换为标准 DataFrame，数据库写入由 sync 层负责。

用法::

    from integrations.base import DataSourceBackend

    class MyBackend:
        name = "my_source"
        def fetch_kline(self, stock_codes, start_date, end_date, **kwargs): ...
        def fetch_minute(self, stock_codes, trade_date, **kwargs): ...
        def fetch_current(self, stock_codes, **kwargs): ...
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class DataSourceBackend(Protocol):
    """数据源后端协议。

    后端只负责：1) 从外部 API 拉取数据；2) 转换为 MySQL 目标表 schema。
    数据库写入（TRUNCATE / DELETE / INSERT）由调用方 sync 层统一负责。

    不是所有后端都需要实现所有方法。不支持某数据类型的后端应抛出
    NotImplementedError。registry 的 has_support() 检查用此判断。
    """

    @property
    def name(self) -> str:
        """短标识符，如 'myquant'、'qmt'、'akshare'。"""
        ...

    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取日K线数据，返回 sm_stock_kline schema DataFrame。

        期望列：stock_code, short_name, trade_time, trade_date,
        k_type, adjust_type, open, close, high, low, volume, amount,
        change, change_pct, turnover_ratio, pre_close。

        调用方会补充 etl_sync_at 后写入。
        """
        ...

    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取分钟K线数据，返回 sm_stock_minute schema DataFrame。

        期望列：stock_code, trade_time, trade_date,
        price, avg_price, change, change_pct, volume, amount。

        调用方会补充 etl_sync_at 后写入。
        """
        ...

    def fetch_current(
        self,
        stock_codes: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        """获取实时行情快照，返回 sm_stock_current schema DataFrame。

        期望列：stock_code, short_name, price, change,
        change_pct, volume, amount, snapshot_at。

        调用方会补充 etl_sync_at 后写入。
        """
        ...
