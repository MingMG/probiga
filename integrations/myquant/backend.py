# -*- coding: utf-8 -*-
"""MyQuant/Goldminer 数据源后端，包装现有 bridge 模块。

不修改 bridge.py / worker.py，只在其上层提供统一 Protocol 接口。
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class MyQuantBackend:
    """MyQuant 数据源后端，基于 gm SDK bridge。"""

    @property
    def name(self) -> str:
        return "myquant"

    def _bridge(self):
        from integrations.myquant import bridge
        if not bridge.is_configured():
            raise RuntimeError(
                "MyQuant 未配置: 需要 GM_TOKEN 和 "
                "runtime/emquant-py36/python.exe"
            )
        return bridge

    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取日K线，返回 sm_stock_kline schema。"""
        bridge = self._bridge()
        supported = [c for c in stock_codes if bridge.to_gm_symbol(c)]
        if not supported:
            return pd.DataFrame()

        short_name_map = kwargs.get("short_name_map", {})
        parts = []
        batch_size = int(os.environ.get("MYQUANT_KLINE_BATCH_SIZE", "50"))
        fields = "symbol,eob,open,high,low,close,volume,amount"

        for i in range(0, len(supported), batch_size):
            batch = supported[i:i + batch_size]
            raw = bridge.history(
                batch,
                frequency="1d",
                start_time=start_date,
                end_time=end_date,
                fields=fields,
                timeout=int(os.environ.get("MYQUANT_TIMEOUT", "300")),
            )
            if raw is not None and not raw.empty:
                sm_df = self._to_kline_schema(raw, short_name_map)
                if not sm_df.empty:
                    parts.append(sm_df)

        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def _to_kline_schema(
        self, df: pd.DataFrame, short_name_map: dict[str, str]
    ) -> pd.DataFrame:
        from integrations.myquant import to_stock_code

        out = pd.DataFrame()
        out["stock_code"] = df["symbol"].map(to_stock_code).astype(str).str.zfill(6)
        trade_time = pd.to_datetime(df["eob"], utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        out["trade_time"] = trade_time.dt.strftime("%Y-%m-%d %H:%M:%S")
        out["trade_date"] = trade_time.dt.strftime("%Y-%m-%d")
        out["short_name"] = out["stock_code"].map(short_name_map).fillna("")
        out["k_type"] = 1
        out["adjust_type"] = 1
        out["open"] = pd.to_numeric(df.get("open"), errors="coerce")
        out["close"] = pd.to_numeric(df.get("close"), errors="coerce")
        out["high"] = pd.to_numeric(df.get("high"), errors="coerce")
        out["low"] = pd.to_numeric(df.get("low"), errors="coerce")
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        out["change"] = None
        out["change_pct"] = None
        out["turnover_ratio"] = None
        out["pre_close"] = None
        return out.dropna(subset=["stock_code", "trade_time"])

    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取分钟K线，返回 sm_stock_minute schema。"""
        bridge = self._bridge()
        supported = [c for c in stock_codes if bridge.to_gm_symbol(c)]
        if not supported:
            return pd.DataFrame()

        start = kwargs.get("start", f"{trade_date} 09:30:00")
        end = kwargs.get("end", f"{trade_date} 15:00:00")
        frequency = kwargs.get("frequency", os.environ.get("MYQUANT_MINUTE_FREQUENCY", "60s").strip() or "60s")
        batch_size = int(os.environ.get("MYQUANT_MINUTE_BATCH_SIZE", "50"))
        fields = "symbol,eob,open,high,low,close,volume,amount"

        parts = []
        for i in range(0, len(supported), batch_size):
            batch = supported[i:i + batch_size]
            raw = bridge.history(
                batch,
                frequency=frequency,
                start_time=start,
                end_time=end,
                fields=fields,
                timeout=int(os.environ.get("MYQUANT_TIMEOUT", "300")),
            )
            if raw is not None and not raw.empty:
                sm_df = self._to_minute_schema(raw)
                if not sm_df.empty:
                    parts.append(sm_df)

        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def _to_minute_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from integrations.myquant import to_stock_code

        out = pd.DataFrame()
        out["stock_code"] = df["symbol"].map(to_stock_code).astype(str).str.zfill(6)
        trade_time = pd.to_datetime(df["eob"], utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        out["trade_time"] = trade_time.dt.strftime("%Y-%m-%d %H:%M:%S")
        out["trade_date"] = trade_time.dt.strftime("%Y-%m-%d")
        out["price"] = pd.to_numeric(df.get("close"), errors="coerce")
        out["avg_price"] = None
        out["change"] = None
        out["change_pct"] = None
        out["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
        return out.dropna(subset=["stock_code", "trade_time"])

    def fetch_current(
        self,
        stock_codes: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        """获取实时行情，返回 sm_stock_current schema。"""
        bridge = self._bridge()
        supported = [c for c in stock_codes if bridge.to_gm_symbol(c)]
        if not supported:
            return pd.DataFrame()

        short_name_map = kwargs.get("short_name_map", {})
        fields = "symbol,price,open,high,low,cum_volume,cum_amount,created_at"

        raw = bridge.current(
            supported,
            fields=fields,
            timeout=int(os.environ.get("MYQUANT_TIMEOUT", "120")),
        )
        if raw is None or raw.empty:
            return pd.DataFrame()

        return self._to_current_schema(raw, short_name_map)

    def _to_current_schema(
        self, df: pd.DataFrame, short_name_map: dict[str, str]
    ) -> pd.DataFrame:
        from integrations.myquant import to_stock_code

        out = pd.DataFrame()
        out["stock_code"] = df["symbol"].map(to_stock_code).astype(str).str.zfill(6)
        out["short_name"] = out["stock_code"].map(short_name_map).fillna("")
        out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
        out["change"] = None
        out["change_pct"] = None
        out["volume"] = pd.to_numeric(df.get("cum_volume"), errors="coerce")
        out["amount"] = pd.to_numeric(df.get("cum_amount"), errors="coerce")
        out["snapshot_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return out.dropna(subset=["stock_code"])


# ---------------------------------------------------------------------------
# 注册
# ---------------------------------------------------------------------------
from integrations.registry import register  # noqa: E402

register("myquant", lambda: MyQuantBackend())
