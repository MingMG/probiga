from __future__ import annotations

import os

import pandas as pd

from integrations.bigqmt import bridge
from integrations.bigqmt.spool import (
    PROVIDER_ID,
    merge_snapshot_frames,
    read_snapshot,
    snapshot_frame,
)
from integrations.qmt.backend import dividend_type_to_adjust_type, to_qmt_symbol
from integrations.registry import register


class BigQmtBackend:
    @property
    def name(self) -> str:
        return "bigqmt"

    @staticmethod
    def _symbols(stock_codes: list[str]) -> list[str]:
        return [symbol for symbol in (to_qmt_symbol(code) for code in stock_codes) if symbol]

    @staticmethod
    def _with_provenance(frame: pd.DataFrame, *, batch_prefix: str) -> pd.DataFrame:
        out = frame.copy()
        received_at = pd.Timestamp.now().to_pydatetime()
        if "qmt_code" not in out.columns:
            out["qmt_code"] = out["stock_code"].map(to_qmt_symbol)
        out["data_source"] = PROVIDER_ID
        out["source_time"] = pd.to_datetime(out.get("trade_time"), errors="coerce")
        out["received_at"] = received_at
        out["batch_id"] = f"{batch_prefix}_{received_at.strftime('%Y%m%d%H%M%S%f')}"
        out["data_version"] = "bigqmt_inner_v2"
        out["quality_status"] = "VERIFIED"
        out["permission_status"] = "SUPPORTED"
        return out

    def fetch_kline(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        symbols = self._symbols(stock_codes)
        if not symbols:
            return pd.DataFrame()
        dividend_type = str(kwargs.get("dividend_type") or os.environ.get("QMT_DIVIDEND_TYPE", "none"))
        capture = bridge.kline_capture(
            symbols,
            start_date=start_date,
            end_date=end_date,
            dividend_type=dividend_type,
            download_history=bool(kwargs.get("download_history", True)),
            batch_size=int(os.environ.get("BIG_QMT_KLINE_BATCH_SIZE", "200")),
            timeout=int(os.environ.get("BIG_QMT_KLINE_TIMEOUT", "600")),
        )
        raw = pd.DataFrame(capture.get("rows") or [])
        capture_proof = {
            key: value for key, value in capture.items() if key != "rows"
        } | {"requested_codes": list(symbols), "row_count": len(raw)}
        if raw.empty:
            empty = pd.DataFrame()
            empty.attrs["bigqmt_capture"] = capture_proof
            return empty
        out = raw.copy()
        out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
        # Standard QMT daily bars report volume in lots. Canonical ProBigA
        # K-line tables and external validators use shares.
        out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce") * 100.0
        names = kwargs.get("short_name_map") or {}
        out["short_name"] = out["stock_code"].map(names).fillna("")
        out["k_type"] = 1
        out["adjust_type"] = dividend_type_to_adjust_type(dividend_type)
        native_pre_close = pd.to_numeric(
            out.get("pre_close"), errors="coerce"
        )
        if "pre_close_origin" not in out.columns:
            out["pre_close_origin"] = "MISSING_NATIVE_QMT"
        out["pre_close_origin"] = out["pre_close_origin"].where(
            out["pre_close_origin"].eq("NATIVE_QMT")
            & native_pre_close.gt(0),
            "MISSING_NATIVE_QMT",
        )
        out["pre_close"] = native_pre_close.where(native_pre_close.gt(0))
        out = self._with_provenance(out, batch_prefix="bigqmt_kline")
        columns = [
            "stock_code", "short_name", "trade_time", "trade_date", "k_type", "adjust_type",
            "open", "close", "high", "low", "volume", "amount", "change", "change_pct",
            "turnover_ratio", "pre_close", "pre_close_origin", "qmt_code", "data_source", "source_time",
            "received_at", "batch_id", "data_version", "quality_status", "permission_status",
        ]
        result = out.reindex(columns=columns)
        result.attrs["bigqmt_capture"] = capture_proof
        return result

    def fetch_minute(
        self,
        stock_codes: list[str],
        trade_date: str,
        **kwargs,
    ) -> pd.DataFrame:
        symbols = self._symbols(stock_codes)
        if not symbols:
            return pd.DataFrame()
        capture = bridge.minute_capture(
            symbols,
            trade_date=trade_date,
            start_date=kwargs.get("start_date", trade_date),
            end_date=kwargs.get("end_date", trade_date),
            count=int(kwargs.get("count", 0) or 0),
            download_history=kwargs.get("download_history", True),
            batch_size=int(os.environ.get("BIG_QMT_MINUTE_BATCH_SIZE", "200")),
            timeout=int(os.environ.get("BIG_QMT_MINUTE_TIMEOUT", "600")),
        )
        raw = pd.DataFrame(capture.get("rows") or [])
        capture_proof = {
            key: value for key, value in capture.items() if key != "rows"
        } | {"requested_codes": list(symbols), "row_count": len(raw)}
        if raw.empty:
            empty = pd.DataFrame()
            empty.attrs["bigqmt_capture"] = capture_proof
            return empty
        out = raw.copy()
        out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
        out = self._with_provenance(out, batch_prefix="bigqmt_minute")
        columns = [
            "stock_code", "trade_time", "trade_date", "price", "avg_price",
            "change", "change_pct", "volume", "amount", "pre_close", "qmt_code",
            "data_source", "source_time", "received_at", "batch_id", "data_version",
            "quality_status", "permission_status",
        ]
        result = out.reindex(columns=columns)
        result.attrs["bigqmt_capture"] = capture_proof
        return result

    def fetch_current(self, stock_codes: list[str], **kwargs) -> pd.DataFrame:
        qmt_home = kwargs.get("qmt_home")
        max_age_seconds = kwargs.get(
            "max_age_seconds",
            float(os.environ.get("BIG_QMT_SNAPSHOT_MAX_AGE_SECONDS", "120")),
        )
        names = kwargs.get("short_name_map") or {}
        full_payload = read_snapshot(
            "full",
            qmt_home=qmt_home,
            max_age_seconds=max_age_seconds,
        )
        tracked_payload = read_snapshot(
            "tracked",
            qmt_home=qmt_home,
            max_age_seconds=max_age_seconds,
        )
        strict_native = bool(kwargs.get("require_native_source_time", False))
        if strict_native and any(
            payload and payload.get("source") != PROVIDER_ID
            for payload in (full_payload, tracked_payload)
        ):
            raise RuntimeError("Full QMT current snapshot source differs")
        frame = merge_snapshot_frames(
            snapshot_frame(full_payload, short_name_map=names,
                           require_native_source_time=strict_native),
            snapshot_frame(tracked_payload, short_name_map=names,
                           require_native_source_time=strict_native),
            prefer_latest_source_time=strict_native,
        )
        if frame.empty or not stock_codes:
            return frame
        wanted = {
            str(code).strip().split(".", 1)[0].zfill(6)
            for code in stock_codes
            if str(code or "").strip()
        }
        return frame.loc[frame["stock_code"].isin(wanted)].reset_index(drop=True)

    def fetch_level1(self, stock_codes: list[str], **kwargs) -> pd.DataFrame:
        """Return only fresh QMT subscription callbacks, never history rows."""

        frame, receipt = bridge.level1_snapshot(
            self._symbols(stock_codes),
            qmt_home=kwargs.get("qmt_home"),
            now=kwargs.get("now"),
            heartbeat_max_age_seconds=float(
                kwargs.get(
                    "heartbeat_max_age_seconds",
                    os.environ.get("BIG_QMT_HEARTBEAT_MAX_AGE_SECONDS", "30"),
                )
            ),
            snapshot_max_age_seconds=float(
                kwargs.get(
                    "snapshot_max_age_seconds",
                    os.environ.get("BIG_QMT_LEVEL1_SNAPSHOT_MAX_AGE_SECONDS", "15"),
                )
            ),
            event_max_age_seconds=float(
                kwargs.get(
                    "event_max_age_seconds",
                    os.environ.get("BIG_QMT_LEVEL1_EVENT_MAX_AGE_SECONDS", "15"),
                )
            ),
            max_ingress_seconds=float(
                kwargs.get(
                    "max_ingress_seconds",
                    os.environ.get("BIG_QMT_LEVEL1_MAX_INGRESS_SECONDS", "15"),
                )
            ),
        )
        if frame.empty:
            frame.attrs["level1_receipt"] = receipt
            return frame
        out = frame.copy()
        out["data_version"] = "bigqmt_live_level1_v1"
        out["quality_status"] = "VERIFIED_LIVE"
        out.attrs["level1_receipt"] = receipt
        return out.reset_index(drop=True)


register("bigqmt", BigQmtBackend)
