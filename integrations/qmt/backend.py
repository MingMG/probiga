# -*- coding: utf-8 -*-
"""QMT (xtquant) 数据源后端。

需要：
  - xtquant 包（pip install xtquant，或从 QMT 安装目录复制）
  - QMT 客户端以 miniQMT 模式运行

环境变量：
  QMT_PYTHON       指向 xtquant 所在 Python 路径（可选，默认用系统 Python）
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 符号转换
# ---------------------------------------------------------------------------
def to_qmt_symbol(code: str) -> str | None:
    """6位 A 股代码 -> QMT 格式（000001.SZ、600519.SH）。"""
    text = str(code or "").strip()
    if not text:
        return None
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return None
    if digits.startswith("6"):
        return f"{digits}.SH"
    if digits.startswith(("0", "3")):
        return f"{digits}.SZ"
    if digits.startswith(("4", "8")):
        return f"{digits}.BJ"
    return None


def from_qmt_symbol(symbol: str) -> str:
    """QMT 格式 -> 6位代码。"""
    return str(symbol).split(".")[0].zfill(6)


def is_configured() -> bool:
    """检查 xtquant 是否可导入且 QMT 客户端可达。"""
    try:
        from xtquant import xtdata  # type: ignore[import-untyped]
        xtdata.connect()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# QMT 后端
# ---------------------------------------------------------------------------
class QmtBackend:
    """QMT 数据源后端，基于 xtquant SDK。"""

    @property
    def name(self) -> str:
        return "qmt"

    def _get_xtdata(self):
        try:
            from xtquant import xtdata  # type: ignore[import-untyped]
            xtdata.connect()
            return xtdata
        except ImportError:
            raise RuntimeError(
                "xtquant 未安装。安装方式: pip install xtquant，"
                "或从 QMT 安装目录复制 xtquant/ 到 site-packages。"
            )

    def _to_qmt_codes(self, stock_codes: list[str]) -> list[str]:
        result = []
        for code in stock_codes:
            sym = to_qmt_symbol(code)
            if sym:
                result.append(sym)
        return result

    def _subscribe(self, xtdata, codes: list[str], period: str) -> None:
        """订阅行情，忽略已订阅的错误。"""
        for code in codes:
            try:
                xtdata.subscribe_quote(code, period=period, count=-1)
            except Exception:
                pass

    # -----------------------------------------------------------------------
    # fetch_kline
    # -----------------------------------------------------------------------
    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取日K线，返回 sm_stock_kline schema。"""
        xtdata = self._get_xtdata()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        self._subscribe(xtdata, qmt_codes, "1d")
        time.sleep(1)

        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="1d",
            count=0,  # 0 = 全部可用
            dividend_type=kwargs.get("dividend_type", "front"),
            fill_data=False,
        )
        if data is None:
            return pd.DataFrame()
        if isinstance(data, pd.DataFrame) and data.empty:
            return pd.DataFrame()

        return self._transform_kline(data, kwargs.get("short_name_map", {}))

    def _transform_kline(
        self, data: Any, short_name_map: dict[str, str]
    ) -> pd.DataFrame:
        rows = []
        if isinstance(data, dict):
            for qmt_code, df in data.items():
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    continue
                code = from_qmt_symbol(qmt_code)
                for idx, row in df.iterrows():
                    trade_date = pd.Timestamp(idx).strftime("%Y-%m-%d")
                    rows.append({
                        "stock_code": code,
                        "short_name": short_name_map.get(code, ""),
                        "trade_time": f"{trade_date} 15:00:00",
                        "trade_date": trade_date,
                        "k_type": 1,
                        "adjust_type": 1,
                        "open": float(row.get("open", 0)),
                        "close": float(row.get("close", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                        "change": None,
                        "change_pct": None,
                        "turnover_ratio": None,
                        "pre_close": None,
                    })
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    # -----------------------------------------------------------------------
    # fetch_minute
    # -----------------------------------------------------------------------
    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        """获取分钟K线，返回 sm_stock_minute schema。"""
        xtdata = self._get_xtdata()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        self._subscribe(xtdata, qmt_codes, "1m")
        time.sleep(1)

        count = kwargs.get("count", 0)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="1m",
            count=count,
            dividend_type="none",
            fill_data=True,
        )
        if data is None:
            return pd.DataFrame()
        if isinstance(data, pd.DataFrame) and data.empty:
            return pd.DataFrame()

        return self._transform_minute(data, trade_date)

    def _transform_minute(
        self, data: Any, trade_date: str
    ) -> pd.DataFrame:
        rows = []
        if isinstance(data, dict):
            for qmt_code, df in data.items():
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    continue
                code = from_qmt_symbol(qmt_code)
                for idx, row in df.iterrows():
                    ts = pd.Timestamp(idx)
                    if str(ts.date()) != trade_date:
                        continue
                    rows.append({
                        "stock_code": code,
                        "trade_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "trade_date": trade_date,
                        "price": float(row.get("close", 0)),
                        "avg_price": None,
                        "change": None,
                        "change_pct": None,
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                    })
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    # -----------------------------------------------------------------------
    # fetch_current
    # -----------------------------------------------------------------------
    def fetch_current(
        self,
        stock_codes: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        """获取实时行情快照，返回 sm_stock_current schema。"""
        xtdata = self._get_xtdata()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        self._subscribe(xtdata, qmt_codes, "1m")
        time.sleep(0.5)

        tick = xtdata.get_full_tick(qmt_codes)
        if tick is None or not isinstance(tick, dict):
            return pd.DataFrame()

        return self._transform_current(tick, kwargs.get("short_name_map", {}))

    def _transform_current(
        self, tick: dict, short_name_map: dict[str, str]
    ) -> pd.DataFrame:
        rows = []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for qmt_code, info in tick.items():
            if not isinstance(info, dict):
                continue
            code = from_qmt_symbol(qmt_code)
            last_price = float(info.get("lastPrice", 0))
            last_close = float(info.get("lastClose", 0))
            change = last_price - last_close
            change_pct = (change / max(last_close, 0.001)) * 100
            rows.append({
                "stock_code": code,
                "short_name": short_name_map.get(code, ""),
                "price": last_price,
                "change": change,
                "change_pct": change_pct,
                "volume": float(info.get("volume", 0)),
                "amount": float(info.get("amount", 0)),
                "snapshot_at": now,
            })
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    # -----------------------------------------------------------------------
    # fetch_tick（QMT 扩展，非 Protocol 标准方法）
    # -----------------------------------------------------------------------
    def fetch_tick(
        self,
        stock_codes: list[str],
        count: int = 100,
        **kwargs,
    ) -> pd.DataFrame:
        """获取逐笔成交数据。QMT 专有扩展。"""
        xtdata = self._get_xtdata()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        self._subscribe(xtdata, qmt_codes, "tick")
        time.sleep(2)

        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="tick",
            count=count,
            dividend_type="none",
            fill_data=True,
        )
        if data is None:
            return pd.DataFrame()
        if isinstance(data, pd.DataFrame):
            return data
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 注册
# ---------------------------------------------------------------------------
from integrations.registry import register  # noqa: E402

register("qmt", lambda: QmtBackend())
