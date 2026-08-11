# -*- coding: utf-8 -*-
"""Synchronize and validate a curated ETF daily-history universe.

The primary source is the signed-in Guojin QMT built-in model (raw and
forward-adjusted daily bars). Raw QMT bars are cross-checked against 10jqka and
Sina before either series is allowed into MySQL. ETF data is intentionally
stored separately from A-share stock tables.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ADATA_ROOT = ensure_adata_import_path(ROOT)

from adata.fund.market.etf_market_ths import ETFMarketThs
from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline
from integrations.bigqmt import bridge as bigqmt_bridge
from integrations.bigqmt.backend import BigQmtBackend
from tools.env_config import create_tool_engine, load_project_env

TENCENT_KLINE_URL = (
    "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    "?param={symbol},day,{start},{end},2000,"
)
_TENCENT_SESSION = requests.Session()
_TENCENT_SESSION.trust_env = False
_TENCENT_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://stockapp.finance.qq.com/",
    }
)


@dataclass(frozen=True)
class ETFMeta:
    code: str
    name: str
    asset_class: str

    @property
    def exchange(self) -> str:
        return exchange_for_code(self.code)

    @property
    def sina_symbol(self) -> str:
        return f"{self.exchange}{self.code}"


# A deliberately small, liquid, cross-asset universe. Eligibility in a
# backtest is still determined point-in-time from each fund's available bars.
ETF_UNIVERSE: tuple[ETFMeta, ...] = (
    ETFMeta("510300", "沪深300ETF", "A股宽基"),
    ETFMeta("510500", "中证500ETF", "A股宽基"),
    ETFMeta("159915", "创业板ETF", "A股宽基"),
    ETFMeta("512100", "中证1000ETF", "A股宽基"),
    ETFMeta("510880", "红利ETF", "A股红利"),
    ETFMeta("512890", "红利低波ETF", "A股红利"),
    ETFMeta("518880", "黄金ETF", "商品"),
    ETFMeta("159985", "豆粕ETF", "商品"),
    ETFMeta("513100", "纳指ETF", "海外权益"),
    ETFMeta("513500", "标普500ETF", "海外权益"),
    ETFMeta("510900", "H股ETF", "港股权益"),
    ETFMeta("511010", "国债ETF", "债券"),
    ETFMeta("511380", "可转债ETF", "债券"),
    ETFMeta("511880", "银华日利ETF", "现金管理"),
)

ETF_META_DDL = """
CREATE TABLE IF NOT EXISTS `si_etf_code` (
  `etf_code` varchar(16) NOT NULL,
  `short_name` varchar(128) NOT NULL,
  `exchange` varchar(8) NOT NULL,
  `asset_class` varchar(32) NOT NULL,
  `list_date` date DEFAULT NULL,
  `last_trade_date` date DEFAULT NULL,
  `status` varchar(16) NOT NULL DEFAULT 'active',
  `primary_source` varchar(32) NOT NULL,
  `validation_source` varchar(32) NOT NULL,
  `sync_status` varchar(16) NOT NULL,
  `updated_at` datetime NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`etf_code`),
  KEY `idx_si_etf_code_status` (`status`,`asset_class`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ETF_KLINE_DDL = """
CREATE TABLE IF NOT EXISTS `sm_etf_kline` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `etf_code` varchar(16) NOT NULL,
  `short_name` varchar(128) NOT NULL,
  `trade_time` datetime NOT NULL,
  `trade_date` date NOT NULL,
  `k_type` tinyint(4) NOT NULL DEFAULT '1',
  `adjust_type` tinyint(4) NOT NULL,
  `open` decimal(18,6) NOT NULL,
  `close` decimal(18,6) NOT NULL,
  `high` decimal(18,6) NOT NULL,
  `low` decimal(18,6) NOT NULL,
  `volume` decimal(24,4) NOT NULL,
  `amount` decimal(24,4) DEFAULT NULL,
  `pre_close` decimal(18,6) DEFAULT NULL,
  `change` decimal(18,6) DEFAULT NULL,
  `change_pct` decimal(18,8) DEFAULT NULL,
  `data_source` varchar(32) NOT NULL,
  `validation_source` varchar(32) NOT NULL,
  `validation_status` varchar(16) NOT NULL,
  `validation_price_max_delta` decimal(18,8) DEFAULT NULL,
  `validation_volume_delta_pct` decimal(18,8) DEFAULT NULL,
  `validation_checked_at` datetime NOT NULL,
  `received_at` datetime NOT NULL,
  `batch_id` varchar(64) NOT NULL,
  `data_version` char(64) NOT NULL,
  `quality_status` varchar(16) NOT NULL,
  `permission_status` varchar(16) NOT NULL DEFAULT 'public',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_sm_etf_kline_bar`
    (`etf_code`,`trade_date`,`k_type`,`adjust_type`),
  KEY `idx_sm_etf_kline_date` (`trade_date`),
  KEY `idx_sm_etf_kline_code_date` (`etf_code`,`trade_date`),
  KEY `idx_sm_etf_kline_quality`
    (`validation_status`,`quality_status`,`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ETF_META_UPSERT = """
INSERT INTO `si_etf_code`
  (`etf_code`,`short_name`,`exchange`,`asset_class`,`list_date`,
   `last_trade_date`,`status`,`primary_source`,`validation_source`,
   `sync_status`,`updated_at`)
VALUES
  (:etf_code,:short_name,:exchange,:asset_class,:list_date,
   :last_trade_date,:status,:primary_source,:validation_source,
   :sync_status,:updated_at)
ON DUPLICATE KEY UPDATE
  `short_name`=VALUES(`short_name`),
  `exchange`=VALUES(`exchange`),
  `asset_class`=VALUES(`asset_class`),
  `list_date`=CASE
    WHEN `list_date` IS NULL THEN VALUES(`list_date`)
    WHEN VALUES(`list_date`) IS NULL THEN `list_date`
    ELSE LEAST(`list_date`, VALUES(`list_date`))
  END,
  `last_trade_date`=CASE
    WHEN `last_trade_date` IS NULL THEN VALUES(`last_trade_date`)
    WHEN VALUES(`last_trade_date`) IS NULL THEN `last_trade_date`
    ELSE GREATEST(`last_trade_date`, VALUES(`last_trade_date`))
  END,
  `status`=VALUES(`status`),
  `primary_source`=VALUES(`primary_source`),
  `validation_source`=VALUES(`validation_source`),
  `sync_status`=VALUES(`sync_status`),
  `updated_at`=VALUES(`updated_at`)
"""

ETF_KLINE_UPSERT = """
INSERT INTO `sm_etf_kline`
  (`etf_code`,`short_name`,`trade_time`,`trade_date`,`k_type`,`adjust_type`,
   `open`,`close`,`high`,`low`,`volume`,`amount`,`pre_close`,`change`,
   `change_pct`,`data_source`,`validation_source`,`validation_status`,
   `validation_price_max_delta`,`validation_volume_delta_pct`,
   `validation_checked_at`,`received_at`,`batch_id`,`data_version`,
   `quality_status`,`permission_status`)
VALUES
  (:etf_code,:short_name,:trade_time,:trade_date,:k_type,:adjust_type,
   :open,:close,:high,:low,:volume,:amount,:pre_close,:change,
   :change_pct,:data_source,:validation_source,:validation_status,
   :validation_price_max_delta,:validation_volume_delta_pct,
   :validation_checked_at,:received_at,:batch_id,:data_version,
   :quality_status,:permission_status)
ON DUPLICATE KEY UPDATE
  `short_name`=VALUES(`short_name`),
  `trade_time`=VALUES(`trade_time`),
  `open`=VALUES(`open`),
  `close`=VALUES(`close`),
  `high`=VALUES(`high`),
  `low`=VALUES(`low`),
  `volume`=VALUES(`volume`),
  `amount`=VALUES(`amount`),
  `pre_close`=VALUES(`pre_close`),
  `change`=VALUES(`change`),
  `change_pct`=VALUES(`change_pct`),
  `data_source`=VALUES(`data_source`),
  `validation_source`=VALUES(`validation_source`),
  `validation_status`=VALUES(`validation_status`),
  `validation_price_max_delta`=VALUES(`validation_price_max_delta`),
  `validation_volume_delta_pct`=VALUES(`validation_volume_delta_pct`),
  `validation_checked_at`=VALUES(`validation_checked_at`),
  `received_at`=VALUES(`received_at`),
  `batch_id`=VALUES(`batch_id`),
  `data_version`=VALUES(`data_version`),
  `quality_status`=VALUES(`quality_status`),
  `permission_status`=VALUES(`permission_status`)
"""


def exchange_for_code(code: str) -> str:
    code = str(code).strip()
    if len(code) != 6 or not code.isdigit():
        raise ValueError(f"invalid ETF code: {code!r}")
    if code.startswith("5"):
        return "sh"
    if code.startswith("1"):
        return "sz"
    raise ValueError(f"unsupported ETF exchange prefix: {code!r}")


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _parse_ths_history_payload(
    raw_text: str,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    lo = raw_text.find("{")
    hi = raw_text.rfind("}")
    if lo < 0 or hi <= lo:
        raise ValueError(f"10jqka returned an invalid payload for {code}")
    payload = json.loads(raw_text[lo : hi + 1])
    if int(payload.get("total") or 0) <= 0 or not payload.get("data"):
        raise ValueError(f"10jqka returned no history for {code}")

    rows = [part.split(",")[:7] for part in payload["data"].split(";") if part]
    frame = pd.DataFrame(
        rows,
        columns=["trade_date", "open", "high", "low", "close", "volume", "amount"],
    )
    frame["trade_date"] = pd.to_datetime(
        frame["trade_date"], format="%Y%m%d", errors="coerce"
    )
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(
        subset=["trade_date", "open", "high", "low", "close", "volume"]
    )
    frame = frame.loc[frame["volume"] > 0].copy()
    frame = frame.loc[
        (frame["trade_date"] >= pd.Timestamp(start_date))
        & (frame["trade_date"] <= pd.Timestamp(end_date))
    ]
    frame = frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["change_pct"] = frame["change"] / frame["pre_close"] * 100.0
    frame.reset_index(drop=True, inplace=True)
    validate_ohlcv(frame, code)
    return frame


def validate_ohlcv(
    frame: pd.DataFrame,
    code: str,
    *,
    require_positive_volume: bool = True,
) -> None:
    if frame.empty:
        raise ValueError(f"empty ETF history for {code}")
    volume_bad = (
        frame["volume"] <= 0
        if require_positive_volume
        else frame["volume"].isna() | (frame["volume"] < 0)
    )
    bad = (
        (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | volume_bad
        | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if bool(bad.any()):
        sample = frame.loc[bad, ["trade_date", "open", "high", "low", "close", "volume"]]
        raise ValueError(f"invalid OHLCV rows for {code}: {sample.head(3).to_dict('records')}")


def fetch_ths_history(
    code: str,
    start_date: str,
    end_date: str,
    *,
    adjust_type: int,
) -> pd.DataFrame:
    if adjust_type not in (0, 1):
        raise ValueError("adjust_type must be 0 (raw) or 1 (forward-adjusted)")
    mode = "00" if adjust_type == 0 else "01"
    url = f"http://d.10jqka.com.cn/v6/line/hs_{code}/{mode}/last36000.js"
    raw_text = ETFMarketThs()._get_text(url, code)
    return _parse_ths_history_payload(raw_text, code, start_date, end_date)


def fetch_qmt_history(
    meta: ETFMeta,
    start_date: str,
    end_date: str,
    *,
    adjust_type: int,
) -> pd.DataFrame:
    """Read ETF history from the standard Guojin QMT built-in model."""
    if adjust_type not in (0, 1):
        raise ValueError("adjust_type must be 0 (raw) or 1 (forward-adjusted)")
    dividend_type = "none" if adjust_type == 0 else "front"
    if not bigqmt_bridge.is_configured():
        raise RuntimeError(
            "Guojin QMT bridge model is not running. In QMT, start "
            "python/probiga_big_qmt_bridge.py before synchronizing ETF history."
        )
    frame = BigQmtBackend().fetch_kline(
        [meta.code],
        start_date,
        end_date,
        dividend_type=dividend_type,
        download_history=True,
        short_name_map={meta.code: meta.name},
    )
    if frame is None or frame.empty:
        raise ValueError(f"Guojin QMT returned no history for {meta.code}")
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pre_close",
        "change",
        "change_pct",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(
        subset=["trade_date", "open", "high", "low", "close", "volume"]
    )
    # Standard QMT may emit calendar-alignment placeholders before listing or
    # on non-trading dates when the built-in reader uses fill_data. They are
    # not market bars and external exchange histories correctly omit them.
    frame = frame.loc[
        (frame["open"] > 0)
        & (frame["high"] > 0)
        & (frame["low"] > 0)
        & (frame["close"] > 0)
        & (frame["volume"] > 0)
    ].copy()
    frame = frame.loc[
        (frame["trade_date"] >= pd.Timestamp(start_date))
        & (frame["trade_date"] <= pd.Timestamp(end_date))
    ]
    frame = frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    if "pre_close" not in frame.columns:
        frame["pre_close"] = frame["close"].shift(1)
    else:
        frame["pre_close"] = frame["pre_close"].where(
            frame["pre_close"] > 0, frame["close"].shift(1)
        )
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["change_pct"] = frame["change"] / frame["pre_close"] * 100.0
    frame.reset_index(drop=True, inplace=True)
    validate_ohlcv(frame, meta.code)
    return frame


def fetch_sina_history(
    meta: ETFMeta,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    frame = fetch_sina_a_daily_kline(
        meta.sina_symbol,
        start_date.replace("-", ""),
        end_date.replace("-", ""),
        "",
        timeout=60.0,
    )
    if frame is None or frame.empty:
        raise ValueError(f"Sina returned no history for {meta.code}")
    frame = frame.rename(columns={"date": "trade_date"})
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(
        subset=["trade_date", "open", "high", "low", "close", "volume"]
    )
    frame = frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    frame.reset_index(drop=True, inplace=True)
    # Sina occasionally publishes valid ETF prices with a zero volume. Keep
    # such rows visible so a third source can arbitrate them.
    validate_ohlcv(frame, meta.code, require_positive_volume=False)
    return frame


def fetch_tencent_history(
    meta: ETFMeta,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    response = _TENCENT_SESSION.get(
        TENCENT_KLINE_URL.format(
            symbol=meta.sina_symbol,
            start=start_date,
            end=end_date,
        ),
        timeout=60.0,
    )
    response.raise_for_status()
    payload = response.json()
    node = (payload.get("data") or {}).get(meta.sina_symbol) or {}
    rows = node.get("day") or node.get("qfqday") or []
    if not rows:
        raise ValueError(f"Tencent returned no history for {meta.code}")
    # Tencent: date, open, close, high, low, volume(lots), ...
    frame = pd.DataFrame(
        [row[:6] for row in rows],
        columns=["trade_date", "open", "close", "high", "low", "volume"],
    )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["volume"] = frame["volume"] * 100.0
    frame["amount"] = pd.NA
    frame = frame.dropna(
        subset=["trade_date", "open", "high", "low", "close", "volume"]
    )
    frame = frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    frame.reset_index(drop=True, inplace=True)
    validate_ohlcv(frame, meta.code)
    return frame


def fetch_external_validation_histories(
    meta: ETFMeta,
    start_date: str,
    end_date: str,
) -> tuple[
    tuple[str, pd.DataFrame],
    tuple[str, pd.DataFrame] | None,
    dict[str, str],
]:
    """Load up to two independent ETF references without one-vendor veto.

    A same-day vendor can legitimately publish later than QMT.  The former
    implementation fetched Sina unconditionally, so a missing Sina bar
    aborted the entire ETF forward job even when Tencent exactly confirmed
    the QMT bar.  Keep every provider failure visible, but proceed when at
    least one independent source can validate QMT.
    """
    sources: list[tuple[str, pd.DataFrame]] = []
    errors: dict[str, str] = {}
    providers = (
        (
            "10jqka",
            lambda: fetch_ths_history(
                meta.code,
                start_date,
                end_date,
                adjust_type=0,
            ),
        ),
        ("sina", lambda: fetch_sina_history(meta, start_date, end_date)),
        (
            "tencent",
            lambda: fetch_tencent_history(meta, start_date, end_date),
        ),
    )
    for source_name, fetcher in providers:
        try:
            frame = fetcher()
            if frame is None or frame.empty:
                raise ValueError("returned no history")
            sources.append((source_name, frame))
        except Exception as exc:
            errors[source_name] = f"{type(exc).__name__}: {exc}"
    if not sources:
        raise ValueError(
            f"all ETF validation sources unavailable for {meta.code}: "
            f"{errors}"
        )
    return (
        sources[0],
        sources[1] if len(sources) > 1 else None,
        errors,
    )


def _source_comparison(
    joined: pd.DataFrame,
    source_suffix: str,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    price_deltas = []
    price_checks = []
    for column in ("open", "high", "low", "close"):
        delta = (
            joined[f"{column}_primary"] - joined[f"{column}_{source_suffix}"]
        ).abs()
        tolerance = 0.011 + joined[f"{column}_primary"].abs() * 0.00005
        price_deltas.append(delta)
        price_checks.append(delta <= tolerance)
    price_max_delta = pd.concat(price_deltas, axis=1).max(axis=1)
    price_ok = pd.concat(price_checks, axis=1).all(axis=1)
    volume_denominator = joined[f"volume_{source_suffix}"].abs().clip(lower=1.0)
    volume_delta_abs = (
        joined["volume_primary"] - joined[f"volume_{source_suffix}"]
    ).abs()
    volume_delta_pct = (
        volume_delta_abs
        / volume_denominator
        * 100.0
    )
    passed = (
        joined[f"close_{source_suffix}"].notna()
        & price_ok
        # QMT daily volume is integral lots. After conversion to shares it can
        # differ from an exchange-source odd-lot total by at most 99 shares.
        & ((volume_delta_abs <= 99.0) | (volume_delta_pct <= 0.10))
    )
    return passed, price_max_delta, volume_delta_pct


def cross_validate_raw(
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    tertiary: pd.DataFrame | None = None,
    *,
    secondary_name: str = "sina",
    tertiary_name: str = "tencent",
    min_overlap_ratio: float = 0.98,
    min_pass_ratio: float = 0.995,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compare raw bars while allowing Sina's documented cent-level rounding."""
    left = primary.copy()
    right = secondary.copy()
    columns = ["trade_date", "open", "high", "low", "close", "volume"]
    joined = left[columns].merge(
        right[columns],
        on="trade_date",
        how="left",
        suffixes=("_primary", "_secondary"),
    )
    overlap = joined["close_secondary"].notna()
    overlap_count = int(overlap.sum())
    overlap_ratio = overlap_count / max(1, len(left))

    secondary_passed, secondary_price_delta, secondary_volume_delta = (
        _source_comparison(joined, "secondary")
    )
    joined["validation_passed"] = secondary_passed
    joined["price_max_delta"] = secondary_price_delta
    joined["volume_delta_pct"] = secondary_volume_delta
    joined["validation_source"] = secondary_name

    if tertiary is not None and not tertiary.empty:
        tertiary_columns = tertiary[columns].rename(
            columns={
                column: f"{column}_tertiary"
                for column in columns
                if column != "trade_date"
            }
        )
        joined = joined.merge(tertiary_columns, on="trade_date", how="left")
        tertiary_passed, tertiary_price_delta, tertiary_volume_delta = (
            _source_comparison(joined, "tertiary")
        )
        fallback = ~joined["validation_passed"] & tertiary_passed
        joined.loc[fallback, "validation_passed"] = True
        joined.loc[fallback, "price_max_delta"] = tertiary_price_delta.loc[fallback]
        joined.loc[fallback, "volume_delta_pct"] = tertiary_volume_delta.loc[fallback]
        joined.loc[fallback, "validation_source"] = tertiary_name

    pass_count = int(joined["validation_passed"].sum())
    tertiary_overlap = (
        joined["close_tertiary"].notna()
        if "close_tertiary" in joined.columns
        else pd.Series(False, index=joined.index)
    )
    any_overlap_count = int((overlap | tertiary_overlap).sum())
    overlap_ratio = any_overlap_count / max(1, len(left))
    pass_ratio = pass_count / max(1, any_overlap_count)

    summary = {
        "primary_rows": int(len(left)),
        "secondary_rows": int(len(right)),
        "secondary_overlap_rows": overlap_count,
        "overlap_rows": any_overlap_count,
        "overlap_ratio": round(overlap_ratio, 8),
        "pass_rows": pass_count,
        "pass_ratio": round(pass_ratio, 8),
        "sina_pass_rows": int(
            (
                joined["validation_passed"]
                & (joined["validation_source"] == secondary_name)
            ).sum()
        ),
        "tencent_fallback_rows": int(
            (
                joined["validation_passed"]
                & (joined["validation_source"] == tertiary_name)
            ).sum()
        ),
        "max_price_delta": _to_float(
            joined.loc[joined["validation_passed"], "price_max_delta"].max()
        ),
        "max_volume_delta_pct": _to_float(
            joined.loc[joined["validation_passed"], "volume_delta_pct"].max()
        ),
    }
    if overlap_ratio < min_overlap_ratio or pass_ratio < min_pass_ratio:
        failed = joined.loc[
            ~joined["validation_passed"],
            [
                "trade_date",
                "close_primary",
                "close_secondary",
                "price_max_delta",
                "volume_delta_pct",
            ],
        ]
        raise ValueError(
            "ETF cross-source validation failed: "
            f"{summary}; sample={failed.head(5).to_dict('records')}"
        )

    validation = joined[
        [
            "trade_date",
            "price_max_delta",
            "volume_delta_pct",
            "validation_passed",
            "validation_source",
        ]
    ].copy()
    return validation, summary


def _data_version(record: dict[str, Any]) -> str:
    canonical = "|".join(
        str(record.get(key) if record.get(key) is not None else "")
        for key in (
            "etf_code",
            "trade_date",
            "adjust_type",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        )
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def prepare_records(
    meta: ETFMeta,
    frame: pd.DataFrame,
    validation: pd.DataFrame,
    *,
    adjust_type: int,
    batch_id: str,
    now: datetime,
) -> list[dict[str, Any]]:
    merged = frame.merge(validation, on="trade_date", how="left")
    records: list[dict[str, Any]] = []
    for row in merged.to_dict(orient="records"):
        validated = bool(row.get("validation_passed"))
        if not validated:
            # A source may have a tiny number of non-overlapping historical
            # dates. They remain excluded rather than silently accepted.
            continue
        record = {
            "etf_code": meta.code,
            "short_name": meta.name,
            "trade_time": pd.Timestamp(row["trade_date"]).to_pydatetime(),
            "trade_date": pd.Timestamp(row["trade_date"]).date(),
            "k_type": 1,
            "adjust_type": adjust_type,
            "open": _to_float(row["open"]),
            "close": _to_float(row["close"]),
            "high": _to_float(row["high"]),
            "low": _to_float(row["low"]),
            "volume": _to_float(row["volume"]),
            "amount": _to_float(row.get("amount")),
            "pre_close": _to_float(row.get("pre_close")),
            "change": _to_float(row.get("change")),
            "change_pct": _to_float(row.get("change_pct")),
            "data_source": "gj_big_qmt_inner",
            "validation_source": str(
                row.get("validation_source") or "10jqka"
            ),
            "validation_status": "passed",
            "validation_price_max_delta": _to_float(row.get("price_max_delta")),
            "validation_volume_delta_pct": _to_float(row.get("volume_delta_pct")),
            "validation_checked_at": now,
            "received_at": now,
            "batch_id": batch_id,
            "quality_status": "validated",
            "permission_status": "public",
        }
        record["data_version"] = _data_version(record)
        records.append(record)
    return records


def ensure_tables(engine: Any) -> None:
    required = {"si_etf_code", "sm_etf_kline"}
    with engine.connect() as conn:
        existing = {
            str(row[0])
            for row in conn.execute(
                text(
                    """
                    SELECT TABLE_NAME FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME IN ('si_etf_code','sm_etf_kline')
                    """
                )
            ).fetchall()
        }
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            "ETF schema migration is required before sync: "
            + ",".join(missing)
        )


def _chunks(records: list[dict[str, Any]], size: int = 500) -> Iterable[list[dict[str, Any]]]:
    for offset in range(0, len(records), size):
        yield records[offset : offset + size]


def upsert_validated_history(
    engine: Any,
    meta: ETFMeta,
    records: list[dict[str, Any]],
    *,
    now: datetime,
) -> None:
    if not records:
        raise ValueError(f"no validated records to write for {meta.code}")
    dates = [record["trade_date"] for record in records]
    meta_record = {
        "etf_code": meta.code,
        "short_name": meta.name,
        "exchange": meta.exchange,
        "asset_class": meta.asset_class,
        "list_date": min(dates),
        "last_trade_date": max(dates),
        "status": "active",
        "primary_source": "gj_big_qmt_inner",
        "validation_source": "10jqka/sina",
        "sync_status": "validated",
        "updated_at": now,
    }
    with engine.begin() as conn:
        conn.execute(text(ETF_META_UPSERT), meta_record)
        for chunk in _chunks(records):
            conn.execute(text(ETF_KLINE_UPSERT), chunk)


def audit_database(engine: Any, expected_codes: set[str]) -> dict[str, Any]:
    placeholders = ",".join(f":c{i}" for i in range(len(expected_codes)))
    params = {f"c{i}": code for i, code in enumerate(sorted(expected_codes))}
    sql = text(
        f"""
        SELECT etf_code,
               COUNT(*) AS row_count,
               COUNT(DISTINCT trade_date) AS trade_days,
               MIN(trade_date) AS min_date,
               MAX(trade_date) AS max_date,
               SUM(CASE WHEN adjust_type=0 THEN 1 ELSE 0 END) AS raw_rows,
               SUM(CASE WHEN adjust_type=1 THEN 1 ELSE 0 END) AS qfq_rows,
               SUM(CASE WHEN adjust_type=0 AND trade_date>='2020-01-01'
                        THEN 1 ELSE 0 END) AS raw_rows_2020,
               SUM(CASE WHEN adjust_type=1 AND trade_date>='2020-01-01'
                        THEN 1 ELSE 0 END) AS qfq_rows_2020,
               SUM(CASE WHEN validation_status<>'passed'
                         OR quality_status<>'validated' THEN 1 ELSE 0 END) AS bad_rows,
               SUM(CASE WHEN high<GREATEST(open,close,low)
                         OR low>LEAST(open,close,high)
                         OR open<=0 OR close<=0 OR volume<=0 THEN 1 ELSE 0 END) AS bad_ohlcv
          FROM sm_etf_kline
         WHERE etf_code IN ({placeholders})
         GROUP BY etf_code
         ORDER BY etf_code
        """
    )
    with engine.connect() as conn:
        rows = [dict(row._mapping) for row in conn.execute(sql, params)]
    returned = {str(row["etf_code"]) for row in rows}
    return {
        "expected_codes": sorted(expected_codes),
        "missing_codes": sorted(expected_codes - returned),
        "codes": rows,
        "passed": (
            returned == expected_codes
            and all(int(row["bad_rows"] or 0) == 0 for row in rows)
            and all(int(row["bad_ohlcv"] or 0) == 0 for row in rows)
            and all(int(row["raw_rows"] or 0) > 0 for row in rows)
            and all(int(row["qfq_rows"] or 0) > 0 for row in rows)
            and all(
                int(row["raw_rows_2020"] or 0) == int(row["qfq_rows_2020"] or 0)
                for row in rows
            )
        ),
    }


def refresh_etf_metadata_dates(engine: Any, expected_codes: set[str]) -> None:
    """Repair metadata bounds from validated unadjusted bars.

    This also makes incremental one-day syncs safe after older versions of the
    tool may have narrowed ``list_date`` to the requested fetch window.
    """
    if not expected_codes:
        return
    placeholders = ",".join(f":c{i}" for i in range(len(expected_codes)))
    params = {f"c{i}": code for i, code in enumerate(sorted(expected_codes))}
    sql = text(
        f"""
        UPDATE si_etf_code AS meta
        JOIN (
          SELECT etf_code,
                 MIN(trade_date) AS min_trade_date,
                 MAX(trade_date) AS max_trade_date
            FROM sm_etf_kline
           WHERE adjust_type = 0
             AND validation_status = 'passed'
             AND quality_status = 'validated'
             AND etf_code IN ({placeholders})
           GROUP BY etf_code
        ) AS bars ON bars.etf_code = meta.etf_code
           SET meta.list_date = bars.min_trade_date,
               meta.last_trade_date = bars.max_trade_date
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, params)


def sync_one(
    meta: ETFMeta,
    start_date: str,
    end_date: str,
    *,
    engine: Any | None,
    write: bool,
    batch_id: str,
) -> dict[str, Any]:
    now = datetime.now().replace(microsecond=0)
    raw = fetch_qmt_history(meta, start_date, end_date, adjust_type=0)
    qfq = fetch_qmt_history(meta, start_date, end_date, adjust_type=1)
    secondary_source, tertiary_source, external_errors = (
        fetch_external_validation_histories(
            meta,
            start_date,
            end_date,
        )
    )
    secondary_name, secondary = secondary_source
    tertiary_name = (
        tertiary_source[0] if tertiary_source is not None else ""
    )
    tertiary = (
        tertiary_source[1] if tertiary_source is not None else None
    )
    validation, check = cross_validate_raw(
        raw,
        secondary,
        tertiary,
        secondary_name=secondary_name,
        tertiary_name=tertiary_name,
    )
    check["external_source_errors"] = external_errors

    raw_records = prepare_records(
        meta, raw, validation, adjust_type=0, batch_id=batch_id, now=now
    )
    qfq_records = prepare_records(
        meta, qfq, validation, adjust_type=1, batch_id=batch_id, now=now
    )
    # A long-lived high-distribution ETF can have non-positive *historical*
    # front-adjusted prices. Keep the complete raw series and only the valid
    # positive adjusted bars; modern-period adjusted coverage is audited
    # separately before backtesting.
    records = raw_records + qfq_records
    if write:
        if engine is None:
            raise ValueError("database engine is required when write=True")
        upsert_validated_history(engine, meta, records, now=now)
    return {
        "meta": asdict(meta),
        "date_from": min(record["trade_date"] for record in records).isoformat(),
        "date_to": max(record["trade_date"] for record in records).isoformat(),
        "validated_trade_days": len(raw_records),
        "raw_trade_days": len(raw_records),
        "qfq_trade_days": len(qfq_records),
        "written_rows": len(records) if write else 0,
        "validation": check,
    }


def _parse_codes(raw: str) -> list[ETFMeta]:
    requested = {part.strip() for part in raw.split(",") if part.strip()}
    known = {meta.code: meta for meta in ETF_UNIVERSE}
    unknown = sorted(requested - set(known))
    if unknown:
        raise ValueError(f"unknown ETF codes: {unknown}")
    return [meta for meta in ETF_UNIVERSE if meta.code in requested]


def main() -> int:
    default_end = (date.today() - timedelta(days=1)).isoformat()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="1990-01-01")
    parser.add_argument("--end", default=default_end)
    parser.add_argument(
        "--codes",
        default=",".join(meta.code for meta in ETF_UNIVERSE),
        help="comma-separated codes from the curated universe",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="create tables and upsert only cross-source validated rows",
    )
    parser.add_argument("--pause", type=float, default=0.25)
    args = parser.parse_args()

    metas = _parse_codes(args.codes)
    batch_id = f"etf-{datetime.now():%Y%m%d%H%M%S}"
    engine = None
    if args.write:
        load_project_env()
        engine = create_tool_engine()
        ensure_tables(engine)

    report: dict[str, Any] = {
        "batch_id": batch_id,
        "write": bool(args.write),
        "start": args.start,
        "end": args.end,
        "results": [],
        "errors": [],
    }
    for index, meta in enumerate(metas):
        try:
            result = sync_one(
                meta,
                args.start,
                args.end,
                engine=engine,
                write=bool(args.write),
                batch_id=batch_id,
            )
            report["results"].append(result)
            print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
        except Exception as exc:
            error = {"code": meta.code, "error": f"{type(exc).__name__}: {exc}"}
            report["errors"].append(error)
            print(json.dumps(error, ensure_ascii=False), flush=True)
        if index + 1 < len(metas) and args.pause > 0:
            time.sleep(args.pause)

    if args.write and engine is not None:
        refresh_etf_metadata_dates(
            engine, {meta.code for meta in metas}
        )
        report["database_audit"] = audit_database(
            engine, {meta.code for meta in metas}
        )
        engine.dispose()
    report["passed"] = not report["errors"] and (
        not args.write or bool(report.get("database_audit", {}).get("passed"))
    )
    print(json.dumps(report, ensure_ascii=False, default=str, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
