from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def to_qmt_symbol(code: str) -> str | None:
    text = str(code or "").strip()
    if not text:
        return None
    if "." in text:
        return text.upper()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return None
    if digits.startswith(("5", "6", "9")):
        return f"{digits}.SH"
    if digits.startswith(("0", "3")):
        return f"{digits}.SZ"
    if digits.startswith(("4", "8")):
        return f"{digits}.BJ"
    return None


def from_qmt_symbol(symbol: str) -> str:
    return str(symbol or "").split(".", 1)[0].zfill(6)


def dividend_type_to_adjust_type(dividend_type: str) -> int:
    text = str(dividend_type or "").strip().lower()
    if text in {"front", "forward", "qfq"}:
        return 1
    if text in {"back", "backward", "hfq"}:
        return 2
    return 0


def is_configured() -> bool:
    try:
        from integrations.qmt import bridge

        if not bridge.is_configured():
            return False
        bridge.ping(timeout=int(os.environ.get("QMT_PING_TIMEOUT", "20")))
        return True
    except Exception:
        return False


def _chunked(items: list[str], size: int) -> list[list[str]]:
    batch_size = max(1, int(size))
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _normalize_qmt_date(value: str, *, include_time: bool, end_of_day: bool = False) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) == 8:
        if include_time:
            return digits + ("235959" if end_of_day else "000000")
        return digits
    return digits


class QmtBackend:
    @property
    def name(self) -> str:
        return "qmt"

    def _bridge(self):
        from integrations.qmt import bridge

        if not bridge.is_configured():
            raise RuntimeError(
                "QMT runtime is not configured. Expected a compatible Python at "
                "runtime/qmt-py313/Scripts/python.exe or set QMT_PYTHON."
            )
        return bridge

    def _to_qmt_codes(self, stock_codes: list[str]) -> list[str]:
        result: list[str] = []
        for code in stock_codes:
            mapped = to_qmt_symbol(code)
            if mapped:
                result.append(mapped)
        return result

    def _batch_size(self, env_name: str, default: int) -> int:
        try:
            return max(1, int(os.environ.get(env_name, str(default)) or default))
        except ValueError:
            return default

    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        bridge = self._bridge()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        short_name_map = kwargs.get("short_name_map", {})
        dividend_type = kwargs.get("dividend_type", os.environ.get("QMT_DIVIDEND_TYPE", "none"))
        adjust_type = dividend_type_to_adjust_type(dividend_type)
        batch_size = self._batch_size("QMT_KLINE_BATCH_SIZE", 300)
        df = bridge.kline(
            qmt_codes,
            start_date=start_date,
            end_date=end_date,
            dividend_type=dividend_type,
            batch_size=batch_size,
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
        df["short_name"] = df["stock_code"].map(short_name_map).fillna("")
        df["adjust_type"] = adjust_type
        cols = [
            "stock_code",
            "short_name",
            "trade_time",
            "trade_date",
            "k_type",
            "adjust_type",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "amount",
            "change",
            "change_pct",
            "turnover_ratio",
            "pre_close",
        ]
        return df.reindex(columns=cols)

    def _transform_kline(
        self,
        data: Any,
        *,
        short_name_map: dict[str, str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        start_ts = pd.Timestamp(start_date).normalize() if start_date else None
        end_ts = pd.Timestamp(end_date).normalize() if end_date else None

        if not isinstance(data, dict):
            return pd.DataFrame()

        for qmt_code, frame in data.items():
            if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            df = frame.copy()
            df.index = pd.to_datetime(df.index, errors="coerce")
            df = df[df.index.notna()].sort_index()
            prev_close = pd.to_numeric(df.get("close"), errors="coerce").shift(1)
            close = pd.to_numeric(df.get("close"), errors="coerce")
            change = close - prev_close
            change_pct = change / prev_close.replace({0: pd.NA}) * 100
            df["_pre_close"] = prev_close
            df["_change"] = change
            df["_change_pct"] = change_pct
            if start_ts is not None:
                df = df[df.index >= start_ts]
            if end_ts is not None:
                df = df[df.index <= end_ts]
            if df.empty:
                continue

            code = from_qmt_symbol(qmt_code)

            for idx, row in df.iterrows():
                trade_date = idx.strftime("%Y-%m-%d")
                rows.append(
                    {
                        "stock_code": code,
                        "short_name": short_name_map.get(code, ""),
                        "trade_time": f"{trade_date} 15:00:00",
                        "trade_date": trade_date,
                        "k_type": 1,
                        "adjust_type": 1,
                        "open": row.get("open"),
                        "close": row.get("close"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "volume": row.get("volume"),
                        "amount": row.get("amount"),
                        "change": row.get("_change"),
                        "change_pct": row.get("_change_pct"),
                        "turnover_ratio": row.get("turnover"),
                        "pre_close": row.get("_pre_close"),
                    }
                )
        return pd.DataFrame(rows)

    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        bridge = self._bridge()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        batch_size = self._batch_size("QMT_MINUTE_BATCH_SIZE", 200)
        count = int(kwargs.get("count", os.environ.get("QMT_MINUTE_COUNT", "0")) or 0)
        start_date = kwargs.get("start_date", trade_date)
        end_date = kwargs.get("end_date", trade_date)
        df = bridge.minute(
            qmt_codes,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            count=count,
            batch_size=batch_size,
            timeout=int(os.environ.get("QMT_TIMEOUT", "300")),
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
        cols = [
            "stock_code",
            "trade_time",
            "trade_date",
            "price",
            "avg_price",
            "change",
            "change_pct",
            "volume",
            "amount",
        ]
        return df.reindex(columns=cols)

    def _transform_minute(self, data: Any, *, trade_date: str) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        if not isinstance(data, dict):
            return pd.DataFrame()

        for qmt_code, frame in data.items():
            if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            code = from_qmt_symbol(qmt_code)
            df = frame.copy()
            df.index = pd.to_datetime(df.index, errors="coerce")
            df = df[df.index.notna()].sort_index()
            df = df[df.index.strftime("%Y-%m-%d") == trade_date]
            if df.empty:
                continue

            for idx, row in df.iterrows():
                rows.append(
                    {
                        "stock_code": code,
                        "trade_time": idx.strftime("%Y-%m-%d %H:%M:%S"),
                        "trade_date": trade_date,
                        "price": row.get("close"),
                        "avg_price": row.get("avgPrice"),
                        "change": None,
                        "change_pct": None,
                        "volume": row.get("volume"),
                        "amount": row.get("amount"),
                    }
                )
        return pd.DataFrame(rows)

    def fetch_current(
        self,
        stock_codes: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        bridge = self._bridge()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()

        short_name_map = kwargs.get("short_name_map", {})
        batch_size = self._batch_size("QMT_CURRENT_BATCH_SIZE", 500)
        df = bridge.current(
            qmt_codes,
            batch_size=batch_size,
            timeout=int(os.environ.get("QMT_TIMEOUT", "120")),
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
        df["short_name"] = df["stock_code"].map(short_name_map).fillna("")
        cols = [
            "stock_code",
            "short_name",
            "price",
            "change",
            "change_pct",
            "volume",
            "amount",
            "snapshot_at",
            "pre_close",
            "timetag",
            "ask_price",
            "ask_vol",
            "bid_price",
            "bid_vol",
            "stock_status",
        ]
        return df.reindex(columns=cols)

    def _transform_current(
        self,
        tick: Any,
        *,
        short_name_map: dict[str, str],
    ) -> pd.DataFrame:
        if not isinstance(tick, dict):
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for qmt_code, info in tick.items():
            if not isinstance(info, dict):
                continue
            code = from_qmt_symbol(qmt_code)
            last_price = float(info.get("lastPrice") or info.get("last_price") or 0)
            last_close = float(info.get("lastClose") or info.get("last_close") or 0)
            if last_price <= 0 and last_close <= 0:
                continue
            if last_price <= 0:
                last_price = last_close
            change = (last_price - last_close) if last_close > 0 else None
            change_pct = ((change / last_close) * 100) if change is not None and last_close > 0 else None
            rows.append(
                {
                    "stock_code": code,
                    "short_name": short_name_map.get(code, ""),
                    "price": last_price,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": info.get("volume"),
                    "amount": info.get("amount"),
                    "snapshot_at": str(info.get("snapshot_at") or ""),
                }
                )
        return pd.DataFrame(rows)

    def fetch_tick(
        self,
        stock_codes: list[str],
        count: int = 100,
        **kwargs,
    ) -> pd.DataFrame:
        bridge = self._bridge()
        qmt_codes = self._to_qmt_codes(stock_codes)
        if not qmt_codes:
            return pd.DataFrame()
        batch_size = self._batch_size("QMT_TICK_BATCH_SIZE", 100)
        return bridge.tick(
            qmt_codes,
            count=count,
            batch_size=batch_size,
            timeout=int(os.environ.get("QMT_TIMEOUT", "180")),
        )


from integrations.registry import register  # noqa: E402

register("qmt", lambda: QmtBackend())
