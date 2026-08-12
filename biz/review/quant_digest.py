#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deterministic, fail-closed quantitative post-market digest.

The legacy review generator mixes data dates and adjustment versions and then
asks prose templates to paper over missing observations.  This module keeps the
opposite boundary: every number is calculated first, publication is allowed
only after the hard gates pass, and rendering never invents a missing metric.

The pure ``generate_quant_digest_from_frames`` entry point is intentionally
independent of SQL.  ``generate_quant_digest`` is the production convenience
entry point that loads the required frames and optionally persists the result.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

from server.common.batch_db import create_batch_engine, read_frame


BEIJING_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_ADJUST_TYPE = 0
QMT_INDUSTRY_SOURCE = "gj_big_qmt_inner"
QMT_VALIDATED = "QMT_VALIDATED"
SW_LEVEL_ONE = "申万一级"
PUBLISH_READY = "ready"
PUBLISH_BLOCKED = "blocked"

FACTOR_SPECS: tuple[tuple[str, str], ...] = (
    ("trend_20d", "技术趋势"),
    ("mean_deviation_20d", "均价偏离"),
    ("volatility_structure_5_20d", "波动结构"),
    ("elasticity_60d", "市场弹性"),
)

_BAR_REQUIRED = {
    "stock_code",
    "trade_date",
    "adjust_type",
    "open",
    "close",
    "pre_close",
    "amount",
}


@dataclass(frozen=True)
class DigestConfig:
    """Publication thresholds and the one permitted K-line version."""

    adjust_type: int = DEFAULT_ADJUST_TYPE
    min_market_coverage: float = 0.98
    min_industry_coverage: float = 0.80
    min_factor_coverage: float = 0.60
    min_industry_members: int = 3
    min_factor_sample: int = 100
    factor_history_sessions: int = 61

    def __post_init__(self) -> None:
        if not isinstance(self.adjust_type, int):
            raise TypeError("adjust_type must be an integer")
        for name in (
            "min_market_coverage",
            "min_industry_coverage",
            "min_factor_coverage",
        ):
            value = float(getattr(self, name))
            if not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        if self.min_industry_members < 1:
            raise ValueError("min_industry_members must be positive")
        if self.min_factor_sample < 5:
            raise ValueError("min_factor_sample must be at least five")
        if self.factor_history_sessions < 20:
            raise ValueError("factor_history_sessions must be at least twenty")


class _Quality:
    def __init__(self, target_date: date, config: DigestConfig) -> None:
        self.target_date = target_date
        self.config = config
        self.gates: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def gate(
        self,
        name: str,
        passed: bool,
        *,
        actual: Any = None,
        expected: Any = None,
        message: str = "",
        hard: bool = True,
    ) -> None:
        item = {
            "name": name,
            "status": "pass" if passed else ("blocked" if hard else "warn"),
            "actual": _json_value(actual),
            "expected": _json_value(expected),
        }
        if message:
            item["message"] = message
        self.gates.append(item)
        if not passed and message:
            (self.errors if hard else self.warnings).append(message)

    def warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    @property
    def blocked(self) -> bool:
        return bool(self.errors)

    def as_dict(self, *, coverage: Mapping[str, Any], source_dates: Mapping[str, Any]) -> dict:
        status = "blocked" if self.blocked else ("warn" if self.warnings else "pass")
        return {
            "status": status,
            "target_date": self.target_date.isoformat(),
            "adjust_type": self.config.adjust_type,
            "gates": self.gates,
            "coverage": {key: _json_value(value) for key, value in coverage.items()},
            "source_dates": {key: _json_value(value) for key, value in source_dates.items()},
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _as_beijing(value: datetime | None) -> datetime:
    result = value or datetime.now(BEIJING_TZ)
    if result.tzinfo is None:
        return result.replace(tzinfo=BEIJING_TZ)
    return result.astimezone(BEIJING_TZ)


def _parse_date(value: str | date | datetime, field: str = "date") -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must use YYYY-MM-DD") from exc
    if parsed.isoformat() != str(value):
        raise ValueError(f"{field} must use YYYY-MM-DD")
    return parsed


def _frame(value: pd.DataFrame | None) -> pd.DataFrame:
    return value.copy(deep=True) if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _normalise_codes(series: pd.Series) -> pd.Series:
    def one(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if raw.endswith((".SH", ".SZ", ".BJ")):
            raw = raw[:-3]
        return raw.zfill(6) if raw.isdigit() and len(raw) < 6 else raw

    return series.map(one)


def _normalise_bars(
    value: pd.DataFrame | None,
    *,
    label: str,
    expected_date: date,
    config: DigestConfig,
    quality: _Quality,
) -> pd.DataFrame:
    bars = _frame(value)
    missing = sorted(_BAR_REQUIRED - set(bars.columns))
    quality.gate(
        f"{label}_schema",
        not missing,
        actual=missing,
        expected="required K-line columns present",
        message=f"{label}行情缺少字段：{', '.join(missing)}" if missing else "",
    )
    if missing:
        return pd.DataFrame(columns=sorted(_BAR_REQUIRED) + ["_return_pct", "_valid_bar"])

    bars["stock_code"] = _normalise_codes(bars["stock_code"])
    parsed_dates = pd.to_datetime(bars["trade_date"], errors="coerce").dt.date
    date_values = sorted({item.isoformat() for item in parsed_dates.dropna()})
    dates_ok = not parsed_dates.isna().any() and date_values == [expected_date.isoformat()]
    quality.gate(
        f"{label}_date",
        dates_ok,
        actual=date_values,
        expected=expected_date.isoformat(),
        message=f"{label}行情日期与目标日期不一致" if not dates_ok else "",
    )
    bars["trade_date"] = parsed_dates

    numeric_adjustments = pd.to_numeric(bars["adjust_type"], errors="coerce")
    adjustments = sorted(
        {
            int(item)
            for item in numeric_adjustments.dropna().tolist()
            if float(item).is_integer()
        }
    )
    adjustment_ok = (
        not numeric_adjustments.isna().any()
        and adjustments == [config.adjust_type]
    )
    quality.gate(
        f"{label}_adjust_type",
        adjustment_ok,
        actual=adjustments,
        expected=[config.adjust_type],
        message=f"{label}行情混入非指定复权版本" if not adjustment_ok else "",
    )

    duplicate_codes = sorted(
        bars.loc[bars.duplicated("stock_code", keep=False), "stock_code"].unique().tolist()
    )
    quality.gate(
        f"{label}_unique_stock",
        not duplicate_codes,
        actual=len(duplicate_codes),
        expected=0,
        message=f"{label}行情存在重复股票记录" if duplicate_codes else "",
    )

    for column in ("open", "close", "pre_close", "amount"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars["_valid_bar"] = (
        (bars["stock_code"] != "")
        & bars["trade_date"].eq(expected_date)
        & numeric_adjustments.eq(config.adjust_type)
        & bars["open"].gt(0)
        & bars["close"].gt(0)
        & bars["pre_close"].gt(0)
        & bars["amount"].ge(0)
    )
    bars["_return_pct"] = (bars["close"] / bars["pre_close"] - 1.0) * 100.0
    bars["_open_gap_pct"] = (bars["open"] / bars["pre_close"] - 1.0) * 100.0
    bars["_intraday_pct"] = (bars["close"] / bars["open"] - 1.0) * 100.0
    return bars


def _expected_codes(universe: pd.DataFrame | Iterable[str] | None, target: date) -> set[str]:
    if universe is None:
        return set()
    if isinstance(universe, pd.DataFrame):
        frame = universe.copy(deep=True)
        if "stock_code" not in frame.columns:
            return set()
        frame["stock_code"] = _normalise_codes(frame["stock_code"])
        if "list_date" in frame.columns:
            listed = pd.to_datetime(frame["list_date"], errors="coerce").dt.date
            frame = frame[listed.isna() | listed.le(target)]
        return {item for item in frame["stock_code"].tolist() if item}
    return {item for item in _normalise_codes(pd.Series(list(universe), dtype="object")) if item}


def _weighted_average(values: pd.Series, weights: pd.Series) -> float | None:
    valid = values.notna() & weights.notna() & weights.gt(0)
    if not valid.any():
        return None
    weight_sum = float(weights[valid].sum())
    if weight_sum <= 0:
        return None
    return float((values[valid] * weights[valid]).sum() / weight_sum)


def _round(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits) if math.isfinite(number) else None


def calc_market_cross_section(
    target_bars: pd.DataFrame,
    previous_bars: pd.DataFrame,
    *,
    shares: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Calculate market breadth and weighted returns from validated bars."""

    current = target_bars[target_bars["_valid_bar"]].copy()
    previous = previous_bars[previous_bars["_valid_bar"]].copy()
    total_amount = float(current["amount"].sum())
    previous_amount = float(previous["amount"].sum())

    market_cap_weighted: float | None = None
    market_cap_coverage: float | None = None
    share_frame = _frame(shares)
    if not share_frame.empty and {"stock_code", "total_shares"}.issubset(share_frame.columns):
        share_frame["stock_code"] = _normalise_codes(share_frame["stock_code"])
        share_frame["total_shares"] = pd.to_numeric(share_frame["total_shares"], errors="coerce")
        share_frame = share_frame.drop_duplicates("stock_code", keep="last")
        weighted = current.merge(share_frame[["stock_code", "total_shares"]], on="stock_code", how="left")
        valid_shares = weighted["total_shares"].gt(0)
        market_cap_coverage = float(valid_shares.mean()) if len(weighted) else 0.0
        weights = weighted["pre_close"] * weighted["total_shares"]
        market_cap_weighted = _weighted_average(weighted["_return_pct"], weights)

    return {
        "status": "available",
        "sample_count": int(len(current)),
        "previous_sample_count": int(len(previous)),
        "return_median_pct": _round(current["_return_pct"].median()),
        "up_coverage": _round(current["_return_pct"].gt(0).mean()),
        "price_weighted_return_pct": _round(
            _weighted_average(current["_return_pct"], current["pre_close"])
        ),
        "amount_weighted_return_pct": _round(
            _weighted_average(current["_return_pct"], current["amount"])
        ),
        "market_cap_weighted_return_pct": _round(market_cap_weighted),
        "market_cap_coverage": _round(market_cap_coverage),
        "open_gap_median_pct": _round(current["_open_gap_pct"].median()),
        "intraday_return_median_pct": _round(current["_intraday_pct"].median()),
        "total_amount": _round(total_amount, 2),
        "previous_total_amount": _round(previous_amount, 2),
        "amount_change_pct": _round(
            (total_amount / previous_amount - 1.0) * 100.0 if previous_amount > 0 else None
        ),
    }


def _normalise_industries(industries: pd.DataFrame | None) -> tuple[pd.DataFrame, list[str]]:
    frame = _frame(industries)
    if frame.empty or not {"stock_code", "industry_name"}.issubset(frame.columns):
        return pd.DataFrame(columns=["stock_code", "industry_name"]), []
    frame = frame[["stock_code", "industry_name"]].copy()
    frame["stock_code"] = _normalise_codes(frame["stock_code"])
    frame["industry_name"] = frame["industry_name"].fillna("").astype(str).str.strip()
    frame = frame[(frame["stock_code"] != "") & (frame["industry_name"] != "")]
    frame = frame.drop_duplicates(["stock_code", "industry_name"])
    counts = frame.groupby("stock_code")["industry_name"].nunique()
    ambiguous = sorted(counts[counts.gt(1)].index.tolist())
    frame = frame[~frame["stock_code"].isin(ambiguous)].drop_duplicates("stock_code")
    return frame.reset_index(drop=True), ambiguous


def _industry_snapshot_contract(
    industries: pd.DataFrame | None,
    *,
    expected_date: date,
    snapshot_date: str | date | None,
    source: str | None,
    quality_status: str | None,
    run_metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Validate one immutable QMT industry snapshot without any fallback."""

    raw = _frame(industries)
    metadata_dates: set[date] = set()
    metadata_sources: set[str] = set()
    metadata_qualities: set[str] = set()
    metadata_types: set[str] = set()
    if not raw.empty:
        if "snapshot_date" in raw.columns:
            parsed = pd.to_datetime(raw["snapshot_date"], errors="coerce").dt.date
            metadata_dates = set(parsed.dropna().tolist())
            if parsed.isna().any():
                metadata_dates.add(date.min)
        if "source" in raw.columns:
            metadata_sources = {
                str(item).strip() for item in raw["source"].dropna().tolist()
            }
        if "quality_status" in raw.columns:
            metadata_qualities = {
                str(item).strip() for item in raw["quality_status"].dropna().tolist()
            }
        if "industry_type" in raw.columns:
            metadata_types = {
                str(item).strip() for item in raw["industry_type"].dropna().tolist()
            }
    explicit_date = (
        _parse_date(snapshot_date, "industry_snapshot_date")
        if snapshot_date is not None
        else None
    )
    date_ok = explicit_date == expected_date and (
        not metadata_dates or metadata_dates == {expected_date}
    )
    source_ok = str(source or "").strip() == QMT_INDUSTRY_SOURCE and (
        not metadata_sources or metadata_sources == {QMT_INDUSTRY_SOURCE}
    )
    quality_ok = str(quality_status or "").strip() == QMT_VALIDATED and (
        not metadata_qualities or metadata_qualities == {QMT_VALIDATED}
    )
    level_ok = not metadata_types or metadata_types == {SW_LEVEL_ONE}
    run = dict(run_metadata or {})
    run_date = None
    try:
        if run.get("snapshot_date") is not None:
            run_date = _parse_date(run["snapshot_date"], "industry_run_snapshot_date")
    except ValueError:
        run_date = None
    try:
        relation_count = int(run.get("industry_relation_count"))
    except (TypeError, ValueError):
        relation_count = -1
    try:
        actual_relation_count = int(run.get("actual_relation_count"))
    except (TypeError, ValueError):
        actual_relation_count = -1
    run_ok = bool(run) and all(
        (
            run_date == expected_date,
            str(run.get("source") or "").strip() == QMT_INDUSTRY_SOURCE,
            str(run.get("quality_status") or "").strip() == QMT_VALIDATED,
            relation_count > 0,
            actual_relation_count == relation_count,
        )
    )
    usable = bool(raw.shape[0]) and all(
        (date_ok, source_ok, quality_ok, level_ok, run_ok)
    )
    membership, ambiguous = _normalise_industries(raw if usable else None)
    return {
        "snapshot_date": explicit_date,
        "source": source,
        "quality_status": quality_status,
        "date_ok": date_ok,
        "source_ok": source_ok,
        "quality_ok": quality_ok,
        "level_ok": level_ok,
        "run_ok": run_ok,
        "run_metadata": run,
        "relation_count": relation_count,
        "actual_relation_count": actual_relation_count,
        "metadata_types": sorted(metadata_types),
        "usable": usable,
        "membership": membership,
        "ambiguous": ambiguous,
    }


def _industry_daily(
    bars: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    min_members: int,
    market_amount: float,
) -> pd.DataFrame:
    merged = bars[bars["_valid_bar"]].merge(membership, on="stock_code", how="inner")
    if merged.empty:
        return pd.DataFrame()
    grouped = merged.groupby("industry_name", sort=False).agg(
        sample_count=("stock_code", "nunique"),
        median_return_pct=("_return_pct", "median"),
        up_coverage=("_return_pct", lambda values: float(values.gt(0).mean())),
        amount=("amount", "sum"),
    )
    grouped = grouped[grouped["sample_count"].ge(min_members)].copy()
    if grouped.empty:
        return grouped
    grouped["amount_share"] = grouped["amount"] / market_amount if market_amount > 0 else None
    grouped = grouped.sort_values(
        ["median_return_pct", "amount", "industry_name"],
        ascending=[False, False, True],
        kind="stable",
    )
    grouped["rank"] = range(1, len(grouped) + 1)
    return grouped.reset_index()


def calc_industry_rotation(
    target_bars: pd.DataFrame,
    previous_bars: pd.DataFrame,
    industries: pd.DataFrame,
    *,
    previous_industries: pd.DataFrame | None = None,
    min_members: int = 3,
) -> dict[str, Any]:
    """Calculate SW level-one breadth and point-in-time rank persistence.

    ``industries`` belongs to the target date.  ``previous_industries`` must be
    the independently captured previous-date snapshot; when it is absent the
    current ranking remains usable but rotation/persistence is deliberately
    reported as unavailable.  Reusing today's membership for yesterday would
    introduce survivorship and classification look-ahead.
    """

    target_amount = float(target_bars.loc[target_bars["_valid_bar"], "amount"].sum())
    previous_amount = float(previous_bars.loc[previous_bars["_valid_bar"], "amount"].sum())
    current = _industry_daily(
        target_bars,
        industries,
        min_members=min_members,
        market_amount=target_amount,
    )
    previous_membership = _frame(previous_industries)
    previous = (
        _industry_daily(
            previous_bars,
            previous_membership,
            min_members=min_members,
            market_amount=previous_amount,
        )
        if not previous_membership.empty
        else pd.DataFrame()
    )
    if current.empty:
        return {"status": "unavailable", "reason": "申万一级行业有效样本不足", "industries": []}

    previous_fields = previous[["industry_name", "median_return_pct", "rank"]].rename(
        columns={"median_return_pct": "previous_median_return_pct", "rank": "previous_rank"}
    ) if not previous.empty else pd.DataFrame(columns=["industry_name", "previous_median_return_pct", "previous_rank"])
    combined = current.merge(previous_fields, on="industry_name", how="left")
    combined["rank_change"] = combined["previous_rank"] - combined["rank"]

    common = combined.dropna(subset=["previous_rank"])
    # Spearman is Pearson correlation over the ranked observations.  Rank
    # explicitly so this path does not make pandas import the optional scipy
    # package at runtime.
    rank_correlation = (
        float(
            common["rank"].rank(method="average").corr(
                common["previous_rank"].rank(method="average")
            )
        )
        if len(common) >= 3
        else None
    )
    current_top3 = combined.head(3)["industry_name"].tolist()
    previous_top3 = previous.head(3)["industry_name"].tolist() if not previous.empty else []
    comparison_available = not previous.empty and len(common) >= 3
    top3_overlap = (
        len(set(current_top3) & set(previous_top3)) if comparison_available else None
    )
    if not comparison_available or rank_correlation is None:
        rotation_state = "数据不足"
    elif top3_overlap >= 2 and rank_correlation >= 0.5:
        rotation_state = "延续"
    elif top3_overlap <= 1 and rank_correlation < 0.3:
        rotation_state = "轮动"
    else:
        rotation_state = "分化"

    def records(frame: pd.DataFrame) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for item in frame.to_dict(orient="records"):
            output.append(
                {
                    "industry_name": str(item["industry_name"]),
                    "sample_count": int(item["sample_count"]),
                    "median_return_pct": _round(item["median_return_pct"]),
                    "up_coverage": _round(item["up_coverage"]),
                    "amount": _round(item["amount"], 2),
                    "amount_share": _round(item["amount_share"]),
                    "rank": int(item["rank"]),
                    "previous_median_return_pct": _round(item.get("previous_median_return_pct")),
                    "previous_rank": int(item["previous_rank"]) if pd.notna(item.get("previous_rank")) else None,
                    "rank_change": int(item["rank_change"]) if pd.notna(item.get("rank_change")) else None,
                }
            )
        return output

    amount_ranked = combined.sort_values("amount", ascending=False).head(5)
    return {
        "status": "available",
        "industry_count": int(len(combined)),
        "previous_comparison_available": comparison_available,
        "rotation_state": rotation_state,
        "rank_correlation": _round(rank_correlation),
        "top3_overlap_count": int(top3_overlap) if top3_overlap is not None else None,
        "top3_overlap": (
            sorted(set(current_top3) & set(previous_top3))
            if comparison_available
            else []
        ),
        "amount_concentration_top5": _round(amount_ranked["amount_share"].sum()),
        "leaders": records(combined.head(5)),
        "laggards": records(combined.tail(3).sort_values("rank", ascending=False)),
        "industries": records(combined),
    }


def build_frozen_factor_exposures(
    history_bars: pd.DataFrame | None,
    frozen_as_of: str | date,
    *,
    adjust_type: int = DEFAULT_ADJUST_TYPE,
    max_sessions: int = 61,
) -> pd.DataFrame:
    """Build factor exposures using data no later than ``frozen_as_of``.

    The resulting frame is a snapshot: one row per stock, an explicit exposure
    date and an explicit maximum source date.  Today's bars are never accepted
    by this function.
    """

    frozen = _parse_date(frozen_as_of, "frozen_as_of")
    frame = _frame(history_bars)
    required = {"stock_code", "trade_date", "adjust_type", "close", "pre_close"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame(columns=["stock_code", "exposure_date", "max_source_date", *[key for key, _ in FACTOR_SPECS]])
    frame["stock_code"] = _normalise_codes(frame["stock_code"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    numeric_adjust = pd.to_numeric(frame["adjust_type"], errors="coerce")
    if numeric_adjust.isna().any() or set(numeric_adjust.astype(int)) != {adjust_type}:
        raise ValueError("factor history contains a non-permitted adjust_type")
    if frame["trade_date"].isna().any() or frame["trade_date"].gt(frozen).any():
        raise ValueError("factor history contains data after the frozen date")
    if frame.duplicated(["stock_code", "trade_date"]).any():
        raise ValueError("factor history contains duplicate stock-date rows")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["pre_close"] = pd.to_numeric(frame["pre_close"], errors="coerce")
    frame = frame[frame["close"].gt(0) & frame["pre_close"].gt(0)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["stock_code", "exposure_date", "max_source_date", *[key for key, _ in FACTOR_SPECS]])
    sessions = sorted(frame["trade_date"].unique())[-max_sessions:]
    frame = frame[frame["trade_date"].isin(sessions)].copy()
    frame["_return"] = frame["close"] / frame["pre_close"] - 1.0
    market_return = frame.groupby("trade_date")["_return"].mean().rename("_market_return")

    output: list[dict[str, Any]] = []
    for stock_code, stock in frame.groupby("stock_code", sort=True):
        stock = stock.sort_values("trade_date").copy()
        last20 = stock.tail(20)
        last5 = stock.tail(5)
        trend = mean_deviation = volatility_structure = elasticity = None
        if len(last20) == 20:
            trend = float((1.0 + last20["_return"]).prod() - 1.0)
            close_mean = float(last20["close"].mean())
            mean_deviation = float(last20.iloc[-1]["close"] / close_mean - 1.0) if close_mean > 0 else None
            if len(last5) == 5:
                volatility_structure = float(
                    last5["_return"].std(ddof=1) - last20["_return"].std(ddof=1)
                )
        beta_sample = stock.tail(60).join(market_return, on="trade_date")
        beta_sample = beta_sample.dropna(subset=["_return", "_market_return"])
        if len(beta_sample) >= 40:
            variance = float(beta_sample["_market_return"].var(ddof=1))
            if variance > 0:
                elasticity = float(beta_sample["_return"].cov(beta_sample["_market_return"]) / variance)
        output.append(
            {
                "stock_code": stock_code,
                "exposure_date": frozen.isoformat(),
                "max_source_date": stock["trade_date"].max().isoformat(),
                "adjust_type": adjust_type,
                "trend_20d": trend,
                "mean_deviation_20d": mean_deviation,
                "volatility_structure_5_20d": volatility_structure,
                "elasticity_60d": elasticity,
            }
        )
    return pd.DataFrame(output)


def calc_frozen_factor_validation(
    target_bars: pd.DataFrame,
    frozen_factors: pd.DataFrame | None,
    *,
    frozen_as_of: str | date,
    adjust_type: int = DEFAULT_ADJUST_TYPE,
    min_coverage: float = 0.60,
    min_sample: int = 100,
) -> dict[str, Any]:
    """Validate frozen exposures against target-day Q5-minus-Q1 returns."""

    frozen = _parse_date(frozen_as_of, "frozen_as_of")
    factors = _frame(frozen_factors)
    current = target_bars[target_bars["_valid_bar"]][["stock_code", "_return_pct"]].copy()
    if factors.empty or "stock_code" not in factors.columns:
        return {
            "status": "unavailable",
            "reason": "T-1冻结因子数据缺失",
            "frozen_as_of": frozen.isoformat(),
            "factors": [],
        }
    factors["stock_code"] = _normalise_codes(factors["stock_code"])
    if factors.duplicated("stock_code").any():
        raise ValueError("frozen factor snapshot contains duplicate stocks")
    if "exposure_date" not in factors.columns:
        raise ValueError("frozen factor snapshot lacks exposure_date")
    exposure_dates = pd.to_datetime(factors["exposure_date"], errors="coerce").dt.date
    if exposure_dates.isna().any() or set(exposure_dates) != {frozen}:
        raise ValueError("factor exposures are not frozen on T-1")
    if "max_source_date" not in factors.columns:
        raise ValueError("frozen factor snapshot lacks max_source_date")
    source_dates = pd.to_datetime(factors["max_source_date"], errors="coerce").dt.date
    if source_dates.isna().any() or source_dates.gt(frozen).any():
        raise ValueError("factor snapshot contains source data after T-1")
    if "adjust_type" not in factors.columns:
        raise ValueError("frozen factor snapshot lacks adjust_type")
    factor_adjustments = pd.to_numeric(factors["adjust_type"], errors="coerce")
    if factor_adjustments.isna().any() or set(factor_adjustments.astype(int)) != {adjust_type}:
        raise ValueError("frozen factor snapshot uses a non-permitted adjust_type")

    merged = current.merge(factors, on="stock_code", how="inner", validate="one_to_one")
    results: list[dict[str, Any]] = []
    denominator = max(len(current), 1)
    for factor_key, label in FACTOR_SPECS:
        if factor_key not in merged.columns:
            continue
        values = pd.to_numeric(merged[factor_key], errors="coerce")
        sample = merged.loc[values.notna(), ["stock_code", "_return_pct"]].copy()
        sample["_factor"] = values[values.notna()].astype(float)
        coverage = len(sample) / denominator
        if len(sample) < min_sample or coverage < min_coverage or sample["_factor"].nunique() < 5:
            continue
        try:
            sample["_quintile"] = pd.qcut(sample["_factor"], q=5, labels=False, duplicates="drop")
        except ValueError:
            continue
        if sample["_quintile"].nunique() != 5:
            continue
        low = float(sample.loc[sample["_quintile"].eq(0), "_return_pct"].mean())
        high = float(sample.loc[sample["_quintile"].eq(4), "_return_pct"].mean())
        rank_ic = sample["_factor"].rank(method="average").corr(
            sample["_return_pct"].rank(method="average")
        )
        results.append(
            {
                "factor_key": factor_key,
                "label": label,
                "sample_count": int(len(sample)),
                "coverage": _round(coverage),
                "q1_return_pct": _round(low),
                "q5_return_pct": _round(high),
                "spread_pct_points": _round(high - low),
                "rank_ic": _round(rank_ic),
            }
        )
    if not results:
        return {
            "status": "unavailable",
            "reason": "T-1冻结因子有效覆盖或分组样本不足",
            "frozen_as_of": frozen.isoformat(),
            "factors": [],
        }
    return {
        "status": "available",
        "frozen_as_of": frozen.isoformat(),
        "method": "按T-1冻结暴露五等分，比较目标日Q5与Q1等权收益",
        "factors": results,
    }


def _pct_direction(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "数据缺失"
    if value > 0:
        return f"上涨{abs(value):.{digits}f}%"
    if value < 0:
        return f"下跌{abs(value):.{digits}f}%"
    return "持平"


def _amount_text(value: float | None) -> str:
    if value is None:
        return "数据缺失"
    if abs(value) >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}万亿元"
    return f"{value / 100_000_000:.2f}亿元"


def _market_paragraph(market: Mapping[str, Any]) -> str:
    up = float(market["up_coverage"])
    median = market.get("return_median_pct")
    if up >= 0.70 and (median or 0) > 0:
        opening = "市场全天呈现普涨结构。"
    elif up >= 0.55:
        opening = "市场全天涨多跌少。"
    elif up >= 0.45:
        opening = "市场全天分化。"
    else:
        opening = "市场全天跌多涨少。"
    weights = [
        f"价格加权{_pct_direction(market.get('price_weighted_return_pct'))}",
        f"成交额加权{_pct_direction(market.get('amount_weighted_return_pct'))}",
    ]
    if market.get("market_cap_weighted_return_pct") is not None:
        weights.insert(1, f"市值加权{_pct_direction(market.get('market_cap_weighted_return_pct'))}")
    amount_change = market.get("amount_change_pct")
    if amount_change is None:
        amount_sentence = "上一交易日成交额不可用，不判断量能变化。"
    elif amount_change > 0.05:
        amount_sentence = f"成交额为{_amount_text(market.get('total_amount'))}，较上一交易日放量{amount_change:.1f}%。"
    elif amount_change < -0.05:
        amount_sentence = f"成交额为{_amount_text(market.get('total_amount'))}，较上一交易日缩量{abs(amount_change):.1f}%。"
    else:
        amount_sentence = f"成交额为{_amount_text(market.get('total_amount'))}，与上一交易日基本持平。"
    return (
        opening
        + f"个股收益中位数{_pct_direction(median)}，上涨覆盖率{up:.1%}；"
        + "，".join(weights)
        + "。"
        + f"开盘缺口中位数为{market.get('open_gap_median_pct'):.2f}%，"
        + f"盘中收益中位数为{market.get('intraday_return_median_pct'):.2f}%。"
        + amount_sentence
    )


def _industry_paragraph(industry: Mapping[str, Any]) -> str:
    if industry.get("status") != "available":
        return f"{industry.get('reason', '申万一级行业数据不足')}，本段不作轮动判断。"
    leaders = industry.get("leaders", [])[:4]
    leader_text = "、".join(
        f"{item['industry_name']}（中位数{_pct_direction(item.get('median_return_pct'))}，成交占比{item.get('amount_share', 0):.1%}）"
        for item in leaders
    )
    state = industry.get("rotation_state")
    correlation = industry.get("rank_correlation")
    if not industry.get("previous_comparison_available"):
        comparison = "上一交易日独立行业快照不可用，延续与轮动结论数据不足。"
    else:
        comparison = (
            f"与上一交易日相比，前三行业重合{industry.get('top3_overlap_count')}个，"
            + (f"行业排名相关系数为{correlation:.2f}，" if correlation is not None else "排名相关数据不足，")
            + f"轮动状态为{state}。"
        )
    return (
        f"申万一级行业中，领涨方向为{leader_text}。"
        + comparison
        + f"成交额前五行业合计占全市场{industry.get('amount_concentration_top5', 0):.1%}；"
        + "该指标仅描述成交集中度，不据此推断资金性质。"
    )


def _factor_paragraph(factor: Mapping[str, Any]) -> str:
    if factor.get("status") != "available":
        return f"{factor.get('reason', 'T-1冻结因子数据不足')}，本段不作方向判断。"
    pieces: list[str] = []
    for item in factor.get("factors", []):
        spread = item.get("spread_pct_points")
        if spread is None:
            continue
        relation = "领先" if spread > 0 else ("落后" if spread < 0 else "持平")
        pieces.append(
            f"{item['label']}高暴露组相对低暴露组{relation}{abs(spread):.2f}个百分点"
            f"（样本{item['sample_count']}只）"
        )
    frozen = _parse_date(factor["frozen_as_of"])
    return (
        f"所有因子暴露均冻结于{frozen.year}年{frozen.month}月{frozen.day}日，"
        + "以目标日收益检验Q5与Q1分组差："
        + "；".join(pieces)
        + "。这些结果是当日横截面检验，不代表统计显著性或未来预测。"
    )


def render_compact_review(result: Mapping[str, Any]) -> str:
    """Render the three-section digest; blocked results deliberately render empty."""

    if result.get("publish_status") != PUBLISH_READY:
        return ""
    target = _parse_date(result["review_date"], "review_date")
    generated = _as_beijing(pd.Timestamp(result["generated_at"]).to_pydatetime())
    quality = result.get("quality_json", {})
    target_coverage = quality.get("coverage", {}).get("target")
    market = result["market_structure_json"]
    data_line = (
        f"数据日期：{target.year}年{target.month:02d}月{target.day:02d}日"
        f"｜有效样本{market.get('sample_count', 0)}只"
        + (f"｜覆盖率{float(target_coverage):.1%}" if target_coverage is not None else "")
    )
    if result.get("data_cutoff_at"):
        cutoff = pd.Timestamp(result["data_cutoff_at"])
        data_line += f"｜数据入库截至{cutoff.strftime('%Y-%m-%d %H:%M:%S')}"
    return "\n\n".join(
        [
            f"北京时间{generated.year}年{generated.month:02d}月{generated.day:02d}日 "
            f"{generated.hour:02d}:{generated.minute:02d} 盘后量化复盘\n{data_line}",
            "大势分析\n" + _market_paragraph(market),
            "行业轮动\n" + _industry_paragraph(result["industry_rotation_json"]),
            "因子特征\n" + _factor_paragraph(result["factor_validation_json"]),
        ]
    )


def _max_cutoff(frame: pd.DataFrame) -> str | None:
    for column in ("etl_sync_at", "received_at", "source_time"):
        if column not in frame.columns:
            continue
        values = pd.to_datetime(frame[column], errors="coerce").dropna()
        if values.empty:
            continue
        value = values.max().to_pydatetime()
        if value.tzinfo is None:
            value = value.replace(tzinfo=BEIJING_TZ)
        return value.astimezone(BEIJING_TZ).isoformat()
    return None


def generate_quant_digest_from_frames(
    date_str: str,
    *,
    target_bars: pd.DataFrame | None,
    previous_bars: pd.DataFrame | None,
    universe: pd.DataFrame | Iterable[str] | None,
    industries: pd.DataFrame | None,
    industry_snapshot_date: str | date | None = None,
    industry_source: str | None = None,
    industry_quality_status: str | None = None,
    previous_industries: pd.DataFrame | None = None,
    previous_industry_snapshot_date: str | date | None = None,
    previous_industry_source: str | None = None,
    previous_industry_quality_status: str | None = None,
    industry_run_metadata: Mapping[str, Any] | None = None,
    previous_industry_run_metadata: Mapping[str, Any] | None = None,
    expected_previous_date: str | date | None = None,
    history_bars: pd.DataFrame | None = None,
    frozen_factors: pd.DataFrame | None = None,
    shares: pd.DataFrame | None = None,
    config: DigestConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a structured digest from frames and enforce publication gates."""

    target = _parse_date(date_str, "date_str")
    config = config or DigestConfig()
    generated = _as_beijing(now)
    quality = _Quality(target, config)

    quality.gate(
        "weekday",
        target.weekday() < 5,
        actual=target.strftime("%A"),
        expected="Monday-Friday",
        message="目标日期是周末，不允许生成交易日复盘" if target.weekday() >= 5 else "",
    )
    target_not_future = target <= generated.date()
    quality.gate(
        "not_future",
        target_not_future,
        actual=target.isoformat(),
        expected=f"不晚于{generated.date().isoformat()}",
        message="目标日期晚于北京时间当前日期" if not target_not_future else "",
    )
    post_close = target < generated.date() or generated.timetz().replace(tzinfo=None) >= time(15, 30)
    quality.gate(
        "post_close",
        post_close,
        actual=generated.isoformat(),
        expected="目标日15:30后",
        message="目标交易日15:30前不允许生成盘后复盘" if not post_close else "",
    )

    raw_previous = _frame(previous_bars)
    previous_dates = (
        sorted(pd.to_datetime(raw_previous.get("trade_date"), errors="coerce").dt.date.dropna().unique())
        if "trade_date" in raw_previous.columns
        else []
    )
    calendar_previous = (
        _parse_date(expected_previous_date, "expected_previous_date")
        if expected_previous_date is not None
        else None
    )
    observed_previous = previous_dates[0] if len(previous_dates) == 1 else None
    previous_date = calendar_previous or observed_previous
    previous_date_ok = bool(
        previous_date is not None
        and previous_date < target
        and observed_previous == previous_date
    )
    quality.gate(
        "previous_trade_date",
        previous_date_ok,
        actual=[item.isoformat() for item in previous_dates],
        expected=(
            calendar_previous.isoformat()
            if calendar_previous is not None
            else "one trading date before target"
        ),
        message="无法确定唯一的上一交易日" if not previous_date_ok else "",
    )
    expected_previous = previous_date or (target if not previous_dates else previous_dates[0])

    target_frame = _normalise_bars(
        target_bars,
        label="target",
        expected_date=target,
        config=config,
        quality=quality,
    )
    previous_frame = _normalise_bars(
        previous_bars,
        label="previous",
        expected_date=expected_previous,
        config=config,
        quality=quality,
    )
    expected = _expected_codes(universe, target)
    quality.gate(
        "universe_present",
        bool(expected),
        actual=len(expected),
        expected="> 0",
        message="目标日股票池为空，无法计算覆盖率" if not expected else "",
    )

    current_valid = target_frame[target_frame["_valid_bar"]]
    previous_valid = previous_frame[previous_frame["_valid_bar"]]
    current_codes = set(current_valid["stock_code"])
    previous_codes = set(previous_valid["stock_code"])
    extra_target = sorted(current_codes - expected) if expected else []
    quality.gate(
        "universe_alignment",
        not extra_target,
        actual=len(extra_target),
        expected=0,
        message="目标日行情含股票池外证券" if extra_target else "",
    )
    denominator = len(expected)
    target_coverage = len(current_codes & expected) / denominator if denominator else 0.0
    previous_coverage = len(previous_codes & expected) / denominator if denominator else 0.0
    quality.gate(
        "target_coverage",
        target_coverage >= config.min_market_coverage,
        actual=_round(target_coverage),
        expected=f">={config.min_market_coverage:.2%}",
        message=f"目标日有效行情覆盖率{target_coverage:.2%}低于门槛" if target_coverage < config.min_market_coverage else "",
    )
    quality.gate(
        "previous_coverage",
        previous_coverage >= config.min_market_coverage,
        actual=_round(previous_coverage),
        expected=f">={config.min_market_coverage:.2%}",
        message=f"上一交易日有效行情覆盖率{previous_coverage:.2%}低于门槛" if previous_coverage < config.min_market_coverage else "",
    )
    target_amount = float(current_valid["amount"].sum()) if not current_valid.empty else 0.0
    previous_amount = float(previous_valid["amount"].sum()) if not previous_valid.empty else 0.0
    quality.gate(
        "positive_turnover",
        target_amount > 0 and previous_amount > 0,
        actual={"target": target_amount, "previous": previous_amount},
        expected="both > 0",
        message="目标日或上一交易日成交额为零，拒绝发布" if target_amount <= 0 or previous_amount <= 0 else "",
    )

    current_snapshot = _industry_snapshot_contract(
        industries,
        expected_date=target,
        snapshot_date=industry_snapshot_date,
        source=industry_source,
        quality_status=industry_quality_status,
        run_metadata=industry_run_metadata,
    )
    previous_snapshot = _industry_snapshot_contract(
        previous_industries,
        expected_date=previous_date if previous_date_ok else target,
        snapshot_date=previous_industry_snapshot_date,
        source=previous_industry_source,
        quality_status=previous_industry_quality_status,
        run_metadata=previous_industry_run_metadata,
    )
    explicit_snapshot_date = current_snapshot["snapshot_date"]
    snapshot_date_ok = current_snapshot["date_ok"]
    snapshot_source_ok = current_snapshot["source_ok"]
    snapshot_quality_ok = current_snapshot["quality_ok"]
    snapshot_type_ok = current_snapshot["level_ok"]
    snapshot_run_ok = current_snapshot["run_ok"]
    snapshot_usable = current_snapshot["usable"]
    quality.gate(
        "industry_snapshot_run_complete",
        snapshot_run_ok,
        actual={
            "row_count": len(_frame(industries)),
            "run": current_snapshot["run_metadata"],
        },
        expected="matching QMT_VALIDATED run metadata and relation count",
        message="目标日行业快照缺少完整运行凭据或关系数不一致"
        if not snapshot_run_ok
        else "",
        hard=False,
    )
    quality.gate(
        "industry_snapshot_same_date",
        snapshot_date_ok,
        actual=explicit_snapshot_date.isoformat() if explicit_snapshot_date else None,
        expected=target.isoformat(),
        message="目标日QMT申万一级行业快照缺失或日期不一致" if not snapshot_date_ok else "",
        hard=False,
    )
    quality.gate(
        "previous_industry_snapshot_run_complete",
        bool(previous_snapshot["run_ok"]),
        actual={
            "row_count": len(_frame(previous_industries)),
            "run": previous_snapshot["run_metadata"],
        },
        expected="matching QMT_VALIDATED run metadata and relation count",
        message="上一交易日行业快照缺少完整运行凭据或关系数不一致"
        if not previous_snapshot["run_ok"]
        else "",
        hard=False,
    )
    quality.gate(
        "industry_snapshot_source",
        snapshot_source_ok,
        actual=industry_source,
        expected=QMT_INDUSTRY_SOURCE,
        message="行业快照不是指定QMT数据源" if not snapshot_source_ok else "",
        hard=False,
    )
    quality.gate(
        "industry_snapshot_quality",
        snapshot_quality_ok,
        actual=industry_quality_status,
        expected=QMT_VALIDATED,
        message="行业快照未达到QMT_VALIDATED" if not snapshot_quality_ok else "",
        hard=False,
    )
    quality.gate(
        "industry_snapshot_level",
        snapshot_type_ok,
        actual=current_snapshot["metadata_types"],
        expected=[SW_LEVEL_ONE],
        message="行业快照混入非申万一级分类" if not snapshot_type_ok else "",
        hard=False,
    )
    membership = current_snapshot["membership"]
    ambiguous = current_snapshot["ambiguous"]
    quality.gate(
        "industry_membership_unique",
        not ambiguous,
        actual=len(ambiguous),
        expected=0,
        message="申万一级行业映射存在一股多行业歧义" if ambiguous else "",
    )
    quality.gate(
        "previous_industry_snapshot_same_date",
        bool(previous_date_ok and previous_snapshot["date_ok"]),
        actual=(
            previous_snapshot["snapshot_date"].isoformat()
            if previous_snapshot["snapshot_date"]
            else None
        ),
        expected=previous_date.isoformat() if previous_date else None,
        message="上一交易日QMT申万一级行业快照缺失或日期不一致"
        if not (previous_date_ok and previous_snapshot["date_ok"])
        else "",
        hard=False,
    )
    quality.gate(
        "previous_industry_snapshot_source",
        bool(previous_snapshot["source_ok"]),
        actual=previous_industry_source,
        expected=QMT_INDUSTRY_SOURCE,
        message="上一交易日行业快照不是指定QMT数据源"
        if not previous_snapshot["source_ok"]
        else "",
        hard=False,
    )
    quality.gate(
        "previous_industry_snapshot_quality",
        bool(previous_snapshot["quality_ok"]),
        actual=previous_industry_quality_status,
        expected=QMT_VALIDATED,
        message="上一交易日行业快照未达到QMT_VALIDATED"
        if not previous_snapshot["quality_ok"]
        else "",
        hard=False,
    )
    quality.gate(
        "previous_industry_snapshot_level",
        bool(previous_snapshot["level_ok"]),
        actual=previous_snapshot["metadata_types"],
        expected=[SW_LEVEL_ONE],
        message="上一交易日行业快照混入非申万一级分类"
        if not previous_snapshot["level_ok"]
        else "",
        hard=False,
    )
    previous_ambiguous = previous_snapshot["ambiguous"]
    quality.gate(
        "previous_industry_membership_unique",
        not previous_ambiguous,
        actual=len(previous_ambiguous),
        expected=0,
        message="上一交易日申万一级行业映射存在一股多行业歧义"
        if previous_ambiguous
        else "",
        hard=False,
    )
    mapped_codes = set(membership["stock_code"])
    industry_coverage = len(current_codes & mapped_codes) / max(len(current_codes), 1)
    previous_mapped_codes = set(previous_snapshot["membership"]["stock_code"])
    previous_industry_coverage = (
        len(previous_codes & previous_mapped_codes) / max(len(previous_codes), 1)
    )
    quality.gate(
        "industry_coverage",
        industry_coverage >= config.min_industry_coverage,
        actual=_round(industry_coverage),
        expected=f">={config.min_industry_coverage:.2%}",
        message=f"申万一级行业映射覆盖率{industry_coverage:.2%}不足，行业段降级" if industry_coverage < config.min_industry_coverage else "",
        hard=False,
    )
    quality.gate(
        "previous_industry_coverage",
        previous_industry_coverage >= config.min_industry_coverage,
        actual=_round(previous_industry_coverage),
        expected=f">={config.min_industry_coverage:.2%}",
        message=(
            f"上一交易日申万一级行业映射覆盖率{previous_industry_coverage:.2%}不足，"
            "不判断行业延续或轮动"
        )
        if previous_industry_coverage < config.min_industry_coverage
        else "",
        hard=False,
    )
    previous_snapshot_usable = bool(
        previous_date_ok
        and previous_snapshot["usable"]
        and not previous_ambiguous
        and previous_industry_coverage >= config.min_industry_coverage
    )

    share_frame = _frame(shares)
    if not share_frame.empty and "change_date" in share_frame.columns:
        share_dates = pd.to_datetime(share_frame["change_date"], errors="coerce").dt.date
        future_shares = share_dates.notna() & share_dates.gt(target)
        quality.gate(
            "shares_as_of",
            not future_shares.any(),
            actual=int(future_shares.sum()),
            expected=0,
            message="市值权重混入目标日之后的股本数据" if future_shares.any() else "",
        )

    market = calc_market_cross_section(target_frame, previous_frame, shares=share_frame)
    if market.get("market_cap_coverage") is not None and market["market_cap_coverage"] < 0.80:
        market["market_cap_weighted_return_pct"] = None
        quality.warn("市值权重覆盖不足80%，正文不展示市值加权收益")

    if snapshot_usable and industry_coverage >= config.min_industry_coverage and not ambiguous:
        industry = calc_industry_rotation(
            target_frame,
            previous_frame,
            membership,
            previous_industries=(
                previous_snapshot["membership"] if previous_snapshot_usable else None
            ),
            min_members=config.min_industry_members,
        )
        if not previous_snapshot_usable:
            quality.warn("上一交易日QMT_VALIDATED申万一级行业快照不可用，不判断行业延续或轮动")
    else:
        industry = {
            "status": "unavailable",
            "reason": (
                "目标日QMT_VALIDATED申万一级行业快照不可用"
                if not snapshot_usable
                else "申万一级行业映射覆盖不足"
            ),
            "industries": [],
        }

    history = _frame(history_bars)
    factors = _frame(frozen_factors)
    if factors.empty and previous_date_ok:
        try:
            factors = build_frozen_factor_exposures(
                history,
                previous_date,
                adjust_type=config.adjust_type,
                max_sessions=config.factor_history_sessions,
            )
        except ValueError as exc:
            quality.gate(
                "factor_history_as_of",
                False,
                actual=str(exc),
                expected=f"adjust_type={config.adjust_type}, source_date<=T-1",
                message=str(exc),
            )
    factor: dict[str, Any]
    if previous_date_ok:
        try:
            factor = calc_frozen_factor_validation(
                target_frame,
                factors,
                frozen_as_of=previous_date,
                adjust_type=config.adjust_type,
                min_coverage=config.min_factor_coverage,
                min_sample=config.min_factor_sample,
            )
        except ValueError as exc:
            quality.gate(
                "factor_snapshot_t_minus_one",
                False,
                actual=str(exc),
                expected=previous_date.isoformat(),
                message=str(exc),
            )
            factor = {
                "status": "unavailable",
                "reason": "T-1冻结因子快照未通过时点校验",
                "frozen_as_of": previous_date.isoformat(),
                "factors": [],
            }
    else:
        factor = {
            "status": "unavailable",
            "reason": "上一交易日缺失，无法冻结因子",
            "frozen_as_of": None,
            "factors": [],
        }
    if factor.get("status") != "available":
        quality.warn(str(factor.get("reason") or "T-1冻结因子不可用"))

    factor_coverages = [item.get("coverage", 0) for item in factor.get("factors", [])]
    coverage = {
        "target": _round(target_coverage),
        "previous": _round(previous_coverage),
        "industry": _round(industry_coverage),
        "industry_previous": _round(previous_industry_coverage),
        "factor_min": _round(min(factor_coverages)) if factor_coverages else 0.0,
        "expected_count": denominator,
        "target_valid_count": len(current_codes & expected),
        "previous_valid_count": len(previous_codes & expected),
    }
    source_dates = {
        "target_bars": target.isoformat(),
        "previous_bars": previous_date.isoformat() if previous_date else None,
        "industry_membership": {
            "current": {
                "snapshot_date": explicit_snapshot_date.isoformat() if explicit_snapshot_date else None,
                "source": industry_source,
                "quality_status": industry_quality_status,
            },
            "previous": {
                "snapshot_date": (
                    previous_snapshot["snapshot_date"].isoformat()
                    if previous_snapshot["snapshot_date"]
                    else None
                ),
                "source": previous_industry_source,
                "quality_status": previous_industry_quality_status,
            },
        },
        "factor_exposure": factor.get("frozen_as_of"),
    }
    quality_json = quality.as_dict(coverage=coverage, source_dates=source_dates)
    result: dict[str, Any] = {
        "review_date": target.isoformat(),
        "adjust_type": config.adjust_type,
        "generated_at": generated.isoformat(),
        "data_cutoff_at": _max_cutoff(target_frame),
        "publish_status": PUBLISH_BLOCKED if quality.blocked else PUBLISH_READY,
        "quality_json": quality_json,
        "market_structure_json": market,
        "industry_rotation_json": industry,
        "factor_validation_json": factor,
        "compact_review": "",
    }
    result["compact_review"] = render_compact_review(result)
    return result


def _default_reader(statement: Any, engine: Any, params: dict[str, Any] | None = None) -> pd.DataFrame:
    return read_frame(statement, engine, params=params)


def load_quant_digest_inputs(
    engine: Any,
    date_str: str,
    *,
    adjust_type: int = DEFAULT_ADJUST_TYPE,
    reader: Callable[[Any, Any, dict[str, Any] | None], pd.DataFrame] = _default_reader,
) -> dict[str, Any]:
    """Load only same-date unadjusted facts required by the pure generator."""

    target = _parse_date(date_str, "date_str").isoformat()
    params = {"d": target, "adjust_type": adjust_type}
    target_bars = reader(
        text(
            """
            SELECT stock_code, trade_date, adjust_type, open, close, high, low,
                   pre_close, amount, volume, etl_sync_at
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1 AND adjust_type = :adjust_type
            ORDER BY stock_code
            """
        ),
        engine,
        params,
    )
    previous_row = reader(
        text(
            """
            SELECT MAX(trade_date) AS trade_date
            FROM si_trade_calendar
            WHERE trade_date < :d AND trade_status = 1
            """
        ),
        engine,
        params,
    )
    previous_date = None
    if not previous_row.empty and pd.notna(previous_row.iloc[0].get("trade_date")):
        previous_date = pd.Timestamp(previous_row.iloc[0]["trade_date"]).date().isoformat()
    previous_bars = pd.DataFrame()
    history_bars = pd.DataFrame()
    if previous_date:
        previous_params = {"d": previous_date, "adjust_type": adjust_type}
        previous_bars = reader(
            text(
                """
                SELECT stock_code, trade_date, adjust_type, open, close, high, low,
                       pre_close, amount, volume, etl_sync_at
                FROM sm_stock_kline
                WHERE trade_date = :d AND k_type = 1 AND adjust_type = :adjust_type
                ORDER BY stock_code
                """
            ),
            engine,
            previous_params,
        )
        history_bars = reader(
            text(
                """
                SELECT stock_code, trade_date, adjust_type, close, pre_close
                FROM sm_stock_kline
                WHERE trade_date BETWEEN DATE_SUB(:d, INTERVAL 120 DAY) AND :d
                  AND k_type = 1 AND adjust_type = :adjust_type
                ORDER BY trade_date, stock_code
                """
            ),
            engine,
            previous_params,
        )

    universe = reader(
        text(
            """
            SELECT stock_code, exchange, list_date
            FROM si_all_code
            WHERE exchange IN ('SH', 'SZ', 'BJ')
              AND (list_date IS NULL OR list_date <= :d)
            ORDER BY stock_code
            """
        ),
        engine,
        {"d": target},
    )
    def load_industry_snapshot(snapshot_date: str | None) -> pd.DataFrame:
        if not snapshot_date:
            return pd.DataFrame()
        return reader(
            text(
                """
                SELECT snapshot_date, source, quality_status, industry_type,
                       stock_code, industry_code, industry_name, captured_at
                FROM qmt_industry_member_snapshot
                WHERE snapshot_date = :d
                  AND source = :source
                  AND quality_status = :quality_status
                  AND industry_type = :industry_type
                ORDER BY stock_code, industry_name
                """
            ),
            engine,
            {
                "d": snapshot_date,
                "source": QMT_INDUSTRY_SOURCE,
                "quality_status": QMT_VALIDATED,
                "industry_type": SW_LEVEL_ONE,
            },
        )

    def load_industry_run(snapshot_date: str | None) -> dict[str, Any] | None:
        if not snapshot_date:
            return None
        frame = reader(
            text(
                """
                SELECT run.snapshot_date, run.source, run.quality_status,
                       run.industry_relation_count, run.captured_at,
                       (
                           SELECT COUNT(*)
                           FROM qmt_industry_member_snapshot member
                           WHERE member.snapshot_date = run.snapshot_date
                             AND member.source = run.source
                             AND member.quality_status = run.quality_status
                       ) AS actual_relation_count
                FROM qmt_membership_snapshot_run run
                WHERE run.snapshot_date = :d
                  AND run.source = :source
                  AND run.quality_status = :quality_status
                LIMIT 1
                """
            ),
            engine,
            {
                "d": snapshot_date,
                "source": QMT_INDUSTRY_SOURCE,
                "quality_status": QMT_VALIDATED,
            },
        )
        return frame.iloc[0].to_dict() if not frame.empty else None

    def industry_metadata(frame: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
        snapshot_date = source = quality_status = None
        if frame.empty:
            return snapshot_date, source, quality_status
        dates = (
            pd.to_datetime(frame.get("snapshot_date"), errors="coerce")
            .dt.date.dropna().unique()
        )
        sources = (
            frame.get("source", pd.Series(dtype="object"))
            .dropna().astype(str).str.strip().unique()
        )
        qualities = (
            frame.get("quality_status", pd.Series(dtype="object"))
            .dropna().astype(str).str.strip().unique()
        )
        if len(dates) == 1:
            snapshot_date = dates[0].isoformat()
        if len(sources) == 1:
            source = sources[0]
        if len(qualities) == 1:
            quality_status = qualities[0]
        return snapshot_date, source, quality_status

    industries = load_industry_snapshot(target)
    previous_industries = load_industry_snapshot(previous_date)
    industry_run_metadata = load_industry_run(target)
    previous_industry_run_metadata = load_industry_run(previous_date)
    (
        industry_snapshot_date,
        industry_source,
        industry_quality_status,
    ) = industry_metadata(industries)
    (
        previous_industry_snapshot_date,
        previous_industry_source,
        previous_industry_quality_status,
    ) = industry_metadata(previous_industries)
    shares = reader(
        text(
            """
            SELECT s.stock_code, s.total_shares, s.change_date
            FROM si_stock_shares s
            INNER JOIN (
                SELECT stock_code, MAX(change_date) AS change_date
                FROM si_stock_shares
                WHERE change_date <= :d
                GROUP BY stock_code
            ) latest
              ON latest.stock_code = s.stock_code
             AND latest.change_date = s.change_date
            ORDER BY s.stock_code
            """
        ),
        engine,
        {"d": target},
    )
    return {
        "target_bars": target_bars,
        "previous_bars": previous_bars,
        "history_bars": history_bars,
        "universe": universe,
        "industries": industries,
        "industry_snapshot_date": industry_snapshot_date,
        "industry_source": industry_source,
        "industry_quality_status": industry_quality_status,
        "previous_industries": previous_industries,
        "previous_industry_snapshot_date": previous_industry_snapshot_date,
        "previous_industry_source": previous_industry_source,
        "previous_industry_quality_status": previous_industry_quality_status,
        "industry_run_metadata": industry_run_metadata,
        "previous_industry_run_metadata": previous_industry_run_metadata,
        "expected_previous_date": previous_date,
        "shares": shares,
    }


_DIGEST_DDL = """
CREATE TABLE IF NOT EXISTS st_quant_review_digest (
    review_date DATE NOT NULL,
    adjust_type INT NOT NULL,
    publish_status VARCHAR(16) NOT NULL,
    compact_review MEDIUMTEXT NOT NULL,
    quality_json LONGTEXT NOT NULL,
    market_structure_json LONGTEXT NOT NULL,
    industry_rotation_json LONGTEXT NOT NULL,
    factor_validation_json LONGTEXT NOT NULL,
    data_cutoff_at DATETIME(6) NULL,
    generated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (review_date, adjust_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_DIGEST_UPSERT = """
INSERT INTO st_quant_review_digest (
    review_date, adjust_type, publish_status, compact_review, quality_json,
    market_structure_json, industry_rotation_json, factor_validation_json,
    data_cutoff_at, generated_at
) VALUES (
    :review_date, :adjust_type, :publish_status, :compact_review, :quality_json,
    :market_structure_json, :industry_rotation_json, :factor_validation_json,
    :data_cutoff_at, :generated_at
)
ON DUPLICATE KEY UPDATE
    compact_review = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        compact_review, VALUES(compact_review)
    ),
    quality_json = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        quality_json, VALUES(quality_json)
    ),
    market_structure_json = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        market_structure_json, VALUES(market_structure_json)
    ),
    industry_rotation_json = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        industry_rotation_json, VALUES(industry_rotation_json)
    ),
    factor_validation_json = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        factor_validation_json, VALUES(factor_validation_json)
    ),
    data_cutoff_at = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        data_cutoff_at, VALUES(data_cutoff_at)
    ),
    generated_at = IF(
        (publish_status = 'ready' AND VALUES(publish_status) = 'blocked') OR
        (publish_status = 'ready' AND VALUES(publish_status) = 'ready' AND
         (VALUES(generated_at) < generated_at OR
          (data_cutoff_at IS NOT NULL AND
           (VALUES(data_cutoff_at) IS NULL OR VALUES(data_cutoff_at) < data_cutoff_at)))),
        generated_at, VALUES(generated_at)
    ),
    publish_status = IF(
        publish_status = 'ready' AND VALUES(publish_status) = 'blocked',
        publish_status, VALUES(publish_status)
    )
"""

_DIGEST_EXISTING_STATUS = """
SELECT publish_status
FROM st_quant_review_digest
WHERE review_date = :review_date AND adjust_type = :adjust_type
FOR UPDATE
"""


def persist_quant_digest(engine: Any, result: Mapping[str, Any]) -> None:
    """Idempotently persist one ready or blocked generation receipt."""

    payload = {
        "review_date": result["review_date"],
        "adjust_type": int(result["adjust_type"]),
        "publish_status": result["publish_status"],
        "compact_review": result.get("compact_review") or "",
        "quality_json": json.dumps(result["quality_json"], ensure_ascii=False, separators=(",", ":")),
        "market_structure_json": json.dumps(result["market_structure_json"], ensure_ascii=False, separators=(",", ":")),
        "industry_rotation_json": json.dumps(result["industry_rotation_json"], ensure_ascii=False, separators=(",", ":")),
        "factor_validation_json": json.dumps(result["factor_validation_json"], ensure_ascii=False, separators=(",", ":")),
        "data_cutoff_at": (
            pd.Timestamp(result["data_cutoff_at"]).to_pydatetime().replace(tzinfo=None)
            if result.get("data_cutoff_at")
            else None
        ),
        "generated_at": _as_beijing(
            pd.Timestamp(result["generated_at"]).to_pydatetime()
        ).replace(tzinfo=None),
    }
    with engine.begin() as connection:
        connection.execute(text(_DIGEST_DDL))
        existing_status = connection.execute(
            text(_DIGEST_EXISTING_STATUS),
            {
                "review_date": payload["review_date"],
                "adjust_type": payload["adjust_type"],
            },
        ).scalar_one_or_none()
        if existing_status == PUBLISH_READY and payload["publish_status"] == PUBLISH_BLOCKED:
            # A later incomplete rerun is a diagnostic event, not a reason to
            # destroy the last publishable artifact.  The guarded UPSERT below
            # closes the no-existing-row race; this lock handles the common
            # existing-row case explicitly inside the same transaction.
            return
        connection.execute(text(_DIGEST_UPSERT), payload)


def generate_quant_digest(
    engine: Any,
    date_str: str,
    persist: bool = True,
    *,
    adjust_type: int = DEFAULT_ADJUST_TYPE,
    min_coverage: float = 0.98,
    now: datetime | None = None,
    min_industry_coverage: float = 0.80,
    min_factor_coverage: float = 0.60,
    min_factor_sample: int = 100,
) -> dict[str, Any]:
    """Load, calculate, gate, render and optionally persist a digest."""

    config = DigestConfig(
        adjust_type=adjust_type,
        min_market_coverage=min_coverage,
        min_industry_coverage=min_industry_coverage,
        min_factor_coverage=min_factor_coverage,
        min_factor_sample=min_factor_sample,
    )
    inputs = load_quant_digest_inputs(engine, date_str, adjust_type=adjust_type)
    result = generate_quant_digest_from_frames(
        date_str,
        **inputs,
        config=config,
        now=now,
    )
    if persist:
        persist_quant_digest(engine, result)
    return result


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="生成三段式盘后量化复盘")
    parser.add_argument("date", help="目标交易日，YYYY-MM-DD")
    parser.add_argument("--no-persist", action="store_true", help="仅预览，不写数据库")
    parser.add_argument("--adjust-type", type=int, default=DEFAULT_ADJUST_TYPE)
    args = parser.parse_args()
    engine = create_batch_engine()
    result = generate_quant_digest(
        engine,
        args.date,
        persist=not args.no_persist,
        adjust_type=args.adjust_type,
    )
    print(result["compact_review"] or json.dumps(result["quality_json"], ensure_ascii=False, indent=2))
    return 0 if result["publish_status"] == PUBLISH_READY else 2


if __name__ == "__main__":
    raise SystemExit(main())
