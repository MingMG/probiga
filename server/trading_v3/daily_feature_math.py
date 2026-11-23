"""Pure native feature calculations; all observations are explicit inputs."""
from __future__ import annotations
import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable
import pandas as pd
from .context_scoring import theme_context_score
from .theme_features import attach_best_theme, calculate_theme_statistics, diversified_universe_codes, infer_name_theme_memberships

PIT_AVAILABLE = 'AVAILABLE'
PIT_DATA_BLOCKED = 'DATA_BLOCKED'
ENTRY_PIT_STATUS_FIELDS = ('finance_pit_status','event_pit_status','industry_pit_status')
DERIVED_CHANGE_PCT_PROTOCOL = 'NATIVE_CLOSE_DIV_NATIVE_PRE_CLOSE_MINUS_ONE_X100_V1'
_FINANCE_NUMERIC_FIELDS = ('net_asset_ps','oper_cf_ps','total_rev_yoy_gr','net_profit_yoy_gr','roe_wtd','gross_margin','net_margin','cash_flow_ratio','asset_liab_ratio')
_MONITORING_ONLY_NUMERIC_FIELDS = frozenset({
    'price', 'latest_low', 'entry_eligible', 'required_position_monitor', 'latest_tradable',
})


def _safe_pct(current: float, base: float) -> float:
    if not base or not math.isfinite(base):
        return 0.0
    return (current / base - 1.0) * 100.0


def _optional_finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _missing_finance_numeric_fields(values: dict[str, Any]) -> list[str]:
    return [
        key
        for key in _FINANCE_NUMERIC_FIELDS
        if values.get(key) is None
        or not math.isfinite(float(values[key]))
    ]


def _derive_change_pct_from_close_pre_close(
    frame: pd.DataFrame,
) -> pd.Series:
    """Never trust the mutable convenience ``change_pct`` column."""

    if not {"close", "pre_close"}.issubset(frame.columns):
        raise RuntimeError(
            "QMT native close/pre_close cannot derive finite change_pct"
        )
    close = pd.to_numeric(frame.get("close"), errors="coerce")
    pre_close = pd.to_numeric(frame.get("pre_close"), errors="coerce")
    invalid = (
        close.isna()
        | pre_close.isna()
        | ~close.map(math.isfinite)
        | ~pre_close.map(math.isfinite)
        | (close <= 0)
        | (pre_close <= 0)
    )
    if bool(invalid.any()):
        raise RuntimeError(
            "QMT native close/pre_close cannot derive finite change_pct"
        )
    result = (close / pre_close - 1.0) * 100.0
    if not bool(result.map(math.isfinite).all()):
        raise RuntimeError("derived QMT change_pct is non-finite")
    return result


def _history_session_evidence(
    observed_dates: Iterable[Any],
    exchange_dates: Iterable[Any],
) -> dict[str, Any]:
    """Bind one stock's available history to the exchange-calendar tail."""

    observed = tuple(
        pd.Timestamp(item).date().isoformat() for item in observed_dates
    )
    calendar = tuple(
        pd.Timestamp(item).date().isoformat() for item in exchange_dates
    )
    expected = (
        calendar[-len(observed):]
        if observed and len(observed) <= len(calendar)
        else ()
    )

    def digest(values: tuple[str, ...]) -> str:
        return hashlib.sha256(
            json.dumps(
                list(values),
                ensure_ascii=False,
                sort_keys=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    return {
        "observed_history_sessions": len(observed),
        "history_sessions_consecutive": bool(
            observed and observed == expected
        ),
        "history_window_start": observed[0] if observed else None,
        "history_window_end": observed[-1] if observed else None,
        "history_session_dates_hash": digest(observed),
        "expected_history_session_dates_hash": digest(expected),
    }


def _rolling_close_drawdown_pct(
    close: pd.Series,
    *,
    window: int,
) -> float:
    """Match the horizon trainer's close-to-rolling-close-maximum formula."""

    if window < 1 or len(close) < window:
        raise ValueError("drawdown window is unavailable")
    values = pd.to_numeric(close.iloc[-window:], errors="coerce")
    latest = float(values.iloc[-1])
    maximum = float(values.max())
    if not math.isfinite(latest) or not math.isfinite(maximum) or maximum <= 0:
        raise ValueError("drawdown close history is invalid")
    return _safe_pct(latest, maximum)


def _snapshot_feature_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {
            str(key): normalized
            for key, child in value.items()
            if (normalized := _snapshot_feature_value(child)) is not None
        }
    if isinstance(value, (list, tuple)):
        return [
            normalized
            for child in value
            if (normalized := _snapshot_feature_value(child)) is not None
        ]
    return None


def _snapshot_feature_payload(
    item: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in item.items():
        normalized = _snapshot_feature_value(value)
        if normalized is not None:
            payload[key] = normalized
    return payload


def _percentile(values: dict[str, float], *, higher_better: bool = True):
    series = pd.Series(
        {
            key: float(value)
            for key, value in values.items()
            if value is not None and math.isfinite(float(value))
        },
        dtype="float64",
    )
    if series.empty:
        return {}
    return series.rank(
        method="average",
        pct=True,
        ascending=higher_better,
    ).to_dict()


def _theme_context_label(item: dict[str, Any]) -> str:
    """Build news-matching text from every point-in-time theme label."""

    raw_names = item.get("theme_names") or ()
    theme_names = (
        raw_names
        if isinstance(raw_names, (list, tuple, set))
        else ()
    )
    return " ".join(
        str(value)
        for value in (
            item.get("theme_code"),
            item.get("theme_name"),
            *theme_names,
        )
        if str(value or "").strip()
    )


def _qmt_attestation_evidence(
    window: dict[str, Any],
    *,
    trade_date: date,
) -> dict[str, Any]:
    """Project the already verified window; never trust run counters alone."""
    evidence = dict(window.get("latest_daily_truth") or {})
    target_rows = int(evidence.get("attested_row_count") or 0)
    schema = str(evidence.get("schema") or "")
    qmt_schema = "probiga.qmt-daily-market-consumer-truth.v1"
    free_schema = "probiga.daily-market-consumer-truth.v1"
    if (
        schema not in {qmt_schema, free_schema}
        or evidence.get("requested_sessions") != [trade_date.isoformat()]
        or not window.get("sessions")
        or window["sessions"][-1] != trade_date.isoformat()
        or target_rows <= 0
    ):
        raise RuntimeError("daily window target evidence differs")
    qmt_current = schema == qmt_schema
    provider = "qmt" if qmt_current else str(evidence.get("provider") or "")
    if not qmt_current and not provider:
        raise RuntimeError("daily window source provider differs")
    return {
        "daily_truth_current": True,
        "daily_truth_schema": schema,
        "daily_provider": provider,
        "qmt_attestation_current": qmt_current,
        "qmt_attestation_status": (
            "COMPLETED" if qmt_current else "NOT_QMT_SOURCE"
        ),
        "qmt_attestation_run_id": str(evidence.get("run_id") or ""),
        "qmt_attestation_start_date": str(
            evidence.get("run_start_date") or ""
        ),
        "qmt_attestation_end_date": str(
            evidence.get("run_end_date") or ""
        ),
        "qmt_attestation_target_rows": target_rows,
        "qmt_attestation_qmt_rows": target_rows,
        "qmt_attestation_matched_rows": target_rows,
        "qmt_attestation_missing_rows": 0,
        "qmt_attestation_mismatched_rows": 0,
    }


def _daily_source_label(market_features: dict[str, Any]) -> str:
    if bool(market_features.get("qmt_attestation_current")):
        prefix = "QMT_ATTESTED_DAILY_KLINE"
    elif bool(market_features.get("daily_truth_current")):
        provider = str(market_features.get("daily_provider") or "UNKNOWN")
        prefix = f"SOURCE_VERIFIED_{provider.upper()}_DAILY_KLINE"
    else:
        prefix = "UNATTESTED_DAILY_KLINE_DATA_BLOCKED"
    return f"{prefix}_PLUS_ASOF_CONCEPT_FUNDAMENTAL_NOTICE"


def normalize_daily_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """One causal price transform for live loading and verified historical windows."""
    frame = frame.sort_values(["stock_code", "trade_date"]).copy()
    numeric = (
        "open",
        "close",
        "high",
        "low",
        "pre_close",
        "amount",
    )
    for column in numeric:
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )
    frame["change_pct"] = _derive_change_pct_from_close_pre_close(frame)
    frame = frame.loc[
        frame[["open", "close", "high", "low"]]
        .gt(0)
        .all(axis=1)
    ].copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str[:6]
    for column in ("open", "close", "high", "low"):
        frame["raw_" + column] = frame[column]
    frame["raw_pre_close"] = frame["pre_close"]
    daily_return = frame["change_pct"] / 100.0
    frame["_growth"] = (1.0 + daily_return).clip(lower=0.01)
    frame["_adj_close"] = frame.groupby(
        "stock_code",
        sort=False,
    )["_growth"].cumprod()
    first_close = frame.groupby("stock_code", sort=False)[
        "raw_close"
    ].transform("first")
    frame["_adj_close"] = frame["_adj_close"] * first_close
    previous_adj_close = frame.groupby(
        "stock_code",
        sort=False,
    )["_adj_close"].shift(1)
    base = frame["pre_close"].where(
        frame["pre_close"] > 0,
        frame["raw_close"],
    )
    for column in ("open", "high", "low"):
        frame[column] = (
            previous_adj_close * (frame["raw_" + column] / base)
        ).where(
            previous_adj_close.notna(),
            frame["_adj_close"]
            * (frame["raw_" + column] / frame["raw_close"]),
        )
    frame["close"] = frame["_adj_close"]
    frame.drop(
        columns=[
            "pre_close",
            "_growth",
            "_adj_close",
        ],
        inplace=True,
        errors="ignore",
    )
    return frame


def _eligible_daily_history_codes(
    frame: pd.DataFrame,
    *,
    expected_trade_date: date,
    required_codes: Iterable[str] = (),
    minimum_history_sessions: int = 65,
) -> list[str]:
    """Keep entry histories exact while retaining held stocks for monitoring.

    A global latest-date check cannot prove that every security has a bar for
    that date.  Ordinary universe members therefore need both sufficient
    history and one exact target-date tail.  Required position codes may be
    retained with an older tail, but the caller must mark them monitoring-only
    before any strategy evaluation.
    """

    if frame.empty:
        return []
    required = {
        str(code).zfill(6)
        for code in required_codes
        if str(code)
    }
    result: list[str] = []
    for raw_code, group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        code = str(raw_code).zfill(6)
        if len(group) < int(minimum_history_sessions):
            continue
        latest = pd.Timestamp(group["trade_date"].max()).date()
        if latest == expected_trade_date or code in required:
            result.append(code)
    return sorted(set(result))


def _restricted_entry_name(value: Any) -> bool:
    """Treat an unavailable target-date name as restricted, not as ordinary."""

    name = str(value or "").strip()
    return not name or "ST" in name.upper() or "退" in name


def _block_entry_candidate_features(
    item: dict[str, Any],
    *,
    reason: str,
) -> None:
    """Preserve risk-monitoring identity but make every sleeve insufficient.

    Strategy sleeves have different required feature sets.  Nulling every
    derived numeric input (apart from the small monitoring allow-list) keeps a
    held security visible to exit/risk code without relying on each current or
    future sleeve to remember a separate stale-data check.
    """

    for key, value in tuple(item.items()):
        if key in _MONITORING_ONLY_NUMERIC_FIELDS or isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            item[key] = None
    item.update(
        {
            "entry_eligible": 0.0,
            "latest_tradable": 0.0,
            "data_quality_status": "DATA_BLOCKED",
            "market_data_quality_status": "DATA_BLOCKED",
            "theme_feature_quality_status": "DATA_BLOCKED",
            "entry_data_quality_reason": str(reason),
        }
    )


def _market_features(
    frame: pd.DataFrame,
    dates: list[date],
    industry_map: dict[str, tuple[str, str]],
) -> dict[str, float]:
    latest = dates[-1]
    returns20 = []
    daily_equal = []
    latest_positive = []
    prior_positive = []
    latest_rows = frame[frame["trade_date"] == latest]
    eligible_codes = {
        str(code)
        for code, count in frame["stock_code"].value_counts().items()
        if int(count) >= 65
    }
    latest_eligible = latest_rows[
        latest_rows["stock_code"].astype(str).isin(eligible_codes)
    ]
    latest_covered_count = int(
        latest_eligible["stock_code"].astype(str).nunique()
    )
    latest_tradable_count = int(
        latest_eligible.loc[
            (pd.to_numeric(latest_eligible["amount"], errors="coerce") > 0)
            & (pd.to_numeric(latest_eligible["close"], errors="coerce") >= 2),
            "stock_code",
        ].astype(str).nunique()
    )
    eligible_count = len(eligible_codes)
    prior_date = dates[-6] if len(dates) >= 6 else dates[0]
    prior_rows = frame[frame["trade_date"] == prior_date]
    latest_positive = (latest_rows["change_pct"] > 0).tolist()
    prior_positive = (prior_rows["change_pct"] > 0).tolist()
    for _code, group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        if len(group) < 21:
            continue
        closes = group["close"].dropna().to_numpy()
        if len(closes) >= 21 and closes[-21] > 0:
            returns20.append(_safe_pct(closes[-1], closes[-21]))
    for trade_date, group in frame.groupby("trade_date"):
        if trade_date in dates[-20:]:
            daily_equal.append(float(group["change_pct"].median()))
    volatility = (
        float(pd.Series(daily_equal).std(ddof=0))
        if daily_equal
        else 0.0
    )
    sector_amount: dict[str, float] = defaultdict(float)
    for row in latest_rows.itertuples():
        industry = industry_map.get(str(row.stock_code), ("", ""))[0]
        if industry:
            sector_amount[industry] += float(row.amount or 0)
    total_amount = sum(sector_amount.values())
    top_sector_share = (
        sum(sorted(sector_amount.values(), reverse=True)[:5])
        / total_amount
        * 100.0
        if total_amount > 0
        else 0.0
    )
    return {
        "market_return_20d_pct": (
            float(pd.Series(returns20).median()) if returns20 else 0.0
        ),
        "market_latest_change_pct": (
            float(latest_rows["change_pct"].median())
            if not latest_rows.empty
            else 0.0
        ),
        "market_breadth_pct": (
            sum(latest_positive) / len(latest_positive) * 100.0
            if latest_positive
            else 0.0
        ),
        "breadth_change_5d_pct": (
            (
                sum(latest_positive) / len(latest_positive)
                - sum(prior_positive) / len(prior_positive)
            )
            * 100.0
            if latest_positive and prior_positive
            else 0.0
        ),
        "realized_volatility_20d_pct": volatility,
        "limit_down_ratio_pct": (
            float((latest_rows["change_pct"] <= -9.5).mean()) * 100.0
            if not latest_rows.empty
            else 0.0
        ),
        "sector_concentration_pct": top_sector_share,
        "market_eligible_stock_count": float(eligible_count),
        "market_latest_stock_count": float(latest_covered_count),
        "market_latest_coverage_ratio": (
            latest_covered_count / eligible_count
            if eligible_count
            else 0.0
        ),
        "market_tradable_coverage_ratio": (
            latest_tradable_count / eligible_count
            if eligible_count
            else 0.0
        ),
    }


def _event_features(
    notices: list[dict[str, Any]],
    *,
    as_of: date,
    return_5d_pct: float,
    amount_ratio: float,
    distance_ma20_pct: float,
) -> dict[str, float]:
    if not notices:
        return {}
    positive_words = (
        "增长",
        "预增",
        "中标",
        "回购",
        "增持",
        "签订",
        "突破",
        "获批",
    )
    negative_words = (
        "减持",
        "亏损",
        "处罚",
        "立案",
        "终止",
        "风险",
        "诉讼",
    )
    latest = notices[0]
    title = str(latest.get("title") or "")
    positive = sum(word in title for word in positive_words)
    negative = sum(word in title for word in negative_words)
    surprise = max(
        0.0,
        min(1.0, 0.5 + 0.18 * positive - 0.25 * negative),
    )
    notice_date = latest.get("notice_date")
    days = (
        max(0, (as_of - notice_date).days)
        if isinstance(notice_date, date)
        else 10
    )
    return {
        "event_surprise": surprise,
        "event_novelty": max(0.0, 1.0 - days / 10.0),
        "event_source_reliability": 0.85,
        "event_price_confirmation": max(
            0.0,
            min(1.0, return_5d_pct / 8.0 + amount_ratio / 5.0),
        ),
        "event_priced_in": max(
            0.0,
            min(1.0, distance_ma20_pct / 20.0),
        ),
        "event_decay": max(0.0, min(1.0, days / 10.0)),
    }


def validate_feature_input(dates, frame):
    if not dates or dates != sorted(set(dates)):
        raise ValueError("native feature sessions must be ordered and unique")
    daily_input_window = frame.attrs["qmt_daily_input_window"]
    if daily_input_window["sessions"] != [d.isoformat() for d in dates]:
        raise ValueError("native feature price window differs from its proof")
    if frame.empty or len(dates) < 65:
        raise RuntimeError("至少需要 65 个已收盘交易日的日 K 数据")


def compute_price_features(*, as_of, dates, frame, context, context_cutoff_at=None, limit=5000,
                           required_codes=(), entry_scope_codes=None):
    validate_feature_input(dates, frame)
    if not isinstance(context.get('research_keywords'), dict):
        raise ValueError('prepared research taxonomy is required')
    required_code_set = {str(code).zfill(6) for code in required_codes if str(code)}
    expected_trade_date = dates[-1]
    daily_input_window = frame.attrs["qmt_daily_input_window"]
    codes = _eligible_daily_history_codes(
        frame,
        expected_trade_date=expected_trade_date,
        required_codes=required_code_set,
    )
    if entry_scope_codes is not None:
        scope = {str(code).zfill(6) for code in entry_scope_codes}
        codes = [code for code in codes if code in scope or code in required_code_set]
    code_set = set(codes)
    if sorted(codes) != sorted(context['codes']):
        raise ValueError('prepared feature identity scope differs')
    industries = dict(context['industries'])
    industry_evidence = dict(context['industry_evidence'])
    theme_memberships = {code:list(values) for code,values in context['theme_memberships'].items()}
    concept_snapshot_date = context['concept_snapshot_date']
    latest_names = {
        str(row.stock_code)[:6]: str(row.short_name or "")
        for row in (
            frame.sort_values("trade_date")
            .groupby("stock_code", sort=False, observed=True)
            .tail(1)
            .itertuples(index=False)
        )
        if str(row.stock_code)[:6] in code_set
    }
    for code, inferred in infer_name_theme_memberships(latest_names).items():
        existing = theme_memberships.setdefault(code, [])
        existing_keys = {(item[0], item[2]) for item in existing}
        for membership in inferred:
            if (membership[0], membership[2]) not in existing_keys:
                existing.append(membership)
    market = _market_features(frame, dates, industries)
    market["qmt_daily_input_window"] = daily_input_window
    change_pct_binding = {
        "protocol": DERIVED_CHANGE_PCT_PROTOCOL,
        "source_fields": ["close", "pre_close"],
        "stored_change_pct_consumed": False,
    }
    market["derived_change_pct_binding"] = {
        **change_pct_binding,
        "binding_hash": hashlib.sha256(
            json.dumps(
                change_pct_binding,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }
    market.update(
        _qmt_attestation_evidence(
            daily_input_window,
            trade_date=expected_trade_date,
        )
    )
    market["concept_snapshot_age_days"] = (
        float((as_of - concept_snapshot_date).days)
        if concept_snapshot_date is not None
        else None
    )
    market.update(context['market_context'])

    base: dict[str, dict[str, Any]] = {}
    strategy_eligible_codes: set[str] = set()
    monitoring_only_reasons: dict[str, str] = {}
    for raw_code, group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        group = group.sort_values("trade_date", kind="mergesort")
        if len(group) < 65:
            continue
        code = str(raw_code).zfill(6)
        if code not in code_set:
            continue
        latest_bar_date = pd.Timestamp(group.iloc[-1]["trade_date"]).date()
        latest_bar_exact = latest_bar_date == expected_trade_date
        name = str(group.iloc[-1]["short_name"] or "")
        name_known = bool(name.strip())
        restricted_name = _restricted_entry_name(name)
        if restricted_name and code not in required_code_set:
            continue
        close = group["close"].astype(float)
        high = group["high"].astype(float)
        low = group["low"].astype(float)
        amount = group["amount"].astype(float)
        # Big QMT emits zero-volume placeholder bars for suspended stocks.
        # They preserve the last price but are not executable market data and
        # must never enter a decision universe.
        entry_data_blocked = (
            not latest_bar_exact
            or close.iloc[-1] < 2
            or amount.iloc[-1] <= 0
            or amount.iloc[-20:].mean() < 50_000_000
        )
        if entry_data_blocked and code not in required_code_set:
            continue
        monitoring_reasons = []
        if not latest_bar_exact:
            monitoring_reasons.append(
                "MISSING_EXACT_TARGET_BAR:"
                f"expected={expected_trade_date.isoformat()},"
                f"actual={latest_bar_date.isoformat()}"
            )
        if not name_known:
            monitoring_reasons.append("TARGET_DATE_STOCK_NAME_UNAVAILABLE")
        elif restricted_name:
            monitoring_reasons.append("RESTRICTED_ST_OR_DELISTING_SECURITY")
        if entry_data_blocked and latest_bar_exact:
            monitoring_reasons.append("TARGET_DATE_ENTRY_MARKET_DATA_BLOCKED")
        if monitoring_reasons:
            monitoring_only_reasons[code] = ";".join(monitoring_reasons)
        else:
            strategy_eligible_codes.add(code)
        ma20 = float(close.iloc[-20:].mean())
        ma20_prior = float(close.iloc[-25:-5].mean())
        ma5 = float(close.iloc[-5:].mean())
        ma60 = float(close.iloc[-60:].mean())
        ma60_prior = (
            float(close.iloc[-70:-10].mean())
            if len(close) >= 70
            else None
        )
        latest_close = float(close.iloc[-1])
        latest_low = float(low.iloc[-1])
        latest_row = group.iloc[-1]
        raw_open = float(latest_row["raw_open"])
        raw_close = float(latest_row["raw_close"])
        raw_high = float(latest_row["raw_high"])
        raw_low = float(latest_row["raw_low"])
        raw_pre_close = float(latest_row["raw_pre_close"])
        ret5 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-6]))
        ret2 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-3]))
        ret20 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-21]))
        ret60 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-61]))
        average_amount_20d = float(amount.iloc[-20:].mean())
        latest_amount = float(group.iloc[-1]["amount"] or 0)
        daily_returns = pd.to_numeric(
            group["change_pct"], errors="coerce"
        ).astype(float)
        history_evidence = _history_session_evidence(
            group["trade_date"].tolist(),
            dates,
        )
        true_ranges = pd.concat(
            [
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_pct = (
            float(true_ranges.iloc[-14:].mean())
            / float(close.iloc[-1])
            * 100.0
        )
        industry_code, industry_name = industries.get(code, ("", ""))
        industry_pit_status = industry_evidence["status_by_code"].get(
            code, PIT_DATA_BLOCKED
        )
        industry_pit_reason = industry_evidence["reason_by_code"].get(
            code, "PIT_INDUSTRY_CODE_NOT_IN_EXACT_SNAPSHOT"
        )
        base[code] = {
            "stock_code": code,
            "stock_name": name or code,
            "latest_trade_date": latest_bar_date.isoformat(),
            "price": float(group.iloc[-1]["raw_close"]),
            "latest_low": float(group.iloc[-1]["raw_low"]),
            "entry_eligible": float(
                code in strategy_eligible_codes
            ),
            "required_position_monitor": float(
                code in required_code_set
            ),
            "latest_tradable": float(amount.iloc[-1] > 0),
            "theme_code": industry_code,
            "theme_name": industry_name,
            "industry_pit_status": industry_pit_status,
            "industry_pit_reason": industry_pit_reason,
            "industry_snapshot_hash": industry_evidence["snapshot_hash"],
            "industry_snapshot_date": industry_evidence.get("snapshot_date"),
            "industry_snapshot_source": industry_evidence.get("source"),
            "industry_rank_eligible": float(
                industry_pit_status == PIT_AVAILABLE
            ),
            **history_evidence,
            "return_2d_pct": ret2,
            "return_1d_pct": float(latest_row["change_pct"] or 0),
            "return_5d_pct": ret5,
            "return_20d_pct": ret20,
            "return_60d_pct": ret60,
            "ma20_slope_5d_pct": _safe_pct(ma20, ma20_prior),
            "ma60_slope_10d_pct": (
                _safe_pct(ma60, ma60_prior)
                if ma60_prior is not None and ma60_prior > 0
                else None
            ),
            "breakout_20d_proximity": min(
                1.0,
                float(close.iloc[-1]) / max(float(high.iloc[-20:].max()), 1e-9),
            ),
            "amount_ratio_5_20": (
                float(amount.iloc[-5:].mean())
                / max(float(amount.iloc[-20:].mean()), 1.0)
            ),
            "amount_ratio_20_60": (
                float(amount.iloc[-20:].mean())
                / max(float(amount.iloc[-60:].mean()), 1.0)
            ),
            "relative_strength_20d_pct": (
                ret20 - market["market_return_20d_pct"]
            ),
            "market_return_20d_pct": market["market_return_20d_pct"],
            "market_latest_change_pct": market[
                "market_latest_change_pct"
            ],
            "latest_relative_to_market_pct": (
                float(group.iloc[-1]["change_pct"] or 0)
                - market["market_latest_change_pct"]
            ),
            "relative_return_1d_pct": (
                float(latest_row["change_pct"] or 0)
                - market["market_latest_change_pct"]
            ),
            "relative_return_20d_pct": (
                ret20 - market["market_return_20d_pct"]
            ),
            "distance_ma20_pct": _safe_pct(
                latest_close,
                ma20,
            ),
            "distance_ma5_pct": _safe_pct(latest_close, ma5),
            "drawdown_20d_pct": _rolling_close_drawdown_pct(
                close,
                window=20,
            ),
            "drawdown_60d_pct": _rolling_close_drawdown_pct(
                close,
                window=60,
            ),
            "overnight_gap_pct": (
                _safe_pct(raw_open, raw_pre_close)
                if raw_pre_close > 0
                else None
            ),
            "intraday_return_pct": (
                _safe_pct(raw_close, raw_open)
                if raw_open > 0
                else None
            ),
            "range_1d_pct": (
                (raw_high - raw_low) / raw_pre_close * 100.0
                if raw_pre_close > 0
                else None
            ),
            "close_location_value": (
                (raw_close - raw_low) / (raw_high - raw_low)
                if raw_high > raw_low
                else 0.5
            ),
            "volatility_5d_pct": float(
                daily_returns.iloc[-5:].std(ddof=0)
            ),
            "volatility_20d_pct": float(
                daily_returns.iloc[-20:].std(ddof=0)
            ),
            "rebound_from_low_pct": _safe_pct(
                latest_close,
                latest_low,
            ),
            "previous_change_pct": float(
                group.iloc[-2]["change_pct"] or 0
            ),
            "close_above_ma20": float(latest_close > ma20),
            "ma20_above_ma60": float(ma20 > ma60),
            "atr_14d_pct": atr_pct,
            "latest_change_pct": float(
                group.iloc[-1]["change_pct"] or 0
            ),
            "amount_ratio_1_20": (
                latest_amount / max(average_amount_20d, 1.0)
            ),
            "latest_amount": latest_amount,
            "average_amount_20d": average_amount_20d,
        }

    member_scores = {
        code: max(
            0.0,
            min(
                1.0,
                0.40
                * (
                    max(
                        0.0,
                        min(
                            1.0,
                            (
                                float(item["relative_strength_20d_pct"])
                                + 5.0
                            )
                            / 25.0,
                        ),
                    )
                )
                + 0.35
                * max(
                    0.0,
                    min(
                        1.0,
                        (
                            float(item["amount_ratio_5_20"])
                            - 0.8
                        )
                        / 1.7,
                    ),
                )
                + 0.25
                * max(
                    0.0,
                    min(
                        1.0,
                        (
                            float(item["return_5d_pct"])
                            + 3.0
                        )
                        / 18.0,
                    ),
                ),
            ),
        )
        for code, item in base.items()
        if code in strategy_eligible_codes
    }
    strategy_frame = frame.loc[
        frame["stock_code"].astype(str).str[:6].isin(strategy_eligible_codes)
    ].copy()
    theme_statistics = calculate_theme_statistics(
        strategy_frame,
        as_of=dates[-1],
        memberships=theme_memberships,
        member_scores=member_scores,
        theme_news_novelty=dict(
            market.get("context_theme_novelty") or {}
        ),
        theme_news_available=bool(
            int(market.get("context_news_count") or 0) > 0
        ),
    )
    attach_best_theme(
        base,
        memberships=theme_memberships,
        statistics=theme_statistics,
        research_keywords=context['research_keywords'],
    )
    for item in base.values():
        item["news_theme_context_score"] = theme_context_score(
            _theme_context_label(item),
            dict(market.get("context_theme_scores") or {}),
        )

    return {
        'as_of': as_of,
        'dates': dates,
        'context_cutoff_at': context_cutoff_at,
        'limit': limit,
        'required_code_set': required_code_set,
        'daily_input_window': daily_input_window,
        'base': base,
        'market': market,
        'strategy_eligible_codes': strategy_eligible_codes,
        'monitoring_only_reasons': monitoring_only_reasons,
        'theme_statistics': theme_statistics,
        'industry_evidence': industry_evidence,
        'concept_snapshot_date': concept_snapshot_date,
    }


def compute_finance_features(state, common_cutoff, financial):
    as_of = state['as_of']
    dates = state['dates']
    context_cutoff_at = state['context_cutoff_at']
    limit = state['limit']
    required_code_set = state['required_code_set']
    daily_input_window = state['daily_input_window']
    base = state['base']
    market = state['market']
    strategy_eligible_codes = state['strategy_eligible_codes']
    monitoring_only_reasons = state['monitoring_only_reasons']
    theme_statistics = state['theme_statistics']
    industry_evidence = state['industry_evidence']
    concept_snapshot_date = state['concept_snapshot_date']
    pit_reader_decision_at = (
        context_cutoff_at
        if common_cutoff.get("status") == PIT_AVAILABLE
        else None
    )
    source_cutoffs = common_cutoff.get("source_cutoff_at") or {}
    finance_cutoff_at = source_cutoffs.get("finance") or common_cutoff.get("fact_cutoff_at") or None
    event_cutoff_at = source_cutoffs.get("event") or common_cutoff.get("fact_cutoff_at") or None
    market["pit_common_cutoff_status"] = common_cutoff.get("status")
    market["pit_common_cutoff_reason"] = common_cutoff.get("reason") or ""
    market["pit_fact_cutoff_at"] = common_cutoff.get("fact_cutoff_at") or ""
    market["pit_source_cutoff_at"] = dict(source_cutoffs)
    market["pit_decision_at"] = common_cutoff.get("decision_at") or ""
    market["pit_common_receipt_root_hash"] = (
        common_cutoff.get("receipt_root_hash") or ""
    )
    event_batch = dict(common_cutoff.get("qmt_event_batch") or {})
    reconstruction = dict(
        event_batch.get("reconstruction_provenance") or {}
    )
    market["pit_reconstruction_mode"] = str(
        event_batch.get("mode") or ""
    )
    market["pit_reconstruction_sha256"] = str(
        event_batch.get("reconstruction_sha256") or ""
    )
    market["pit_reconstructed_at"] = str(
        reconstruction.get("reconstructed_at") or ""
    )
    market["pit_reconstruction_provenance"] = reconstruction
    if not set(base) <= financial.keys():
        raise ValueError('prepared feature finance scope differs')
    finance = {code:financial[code] for code in base}
    quality_raw = {}
    growth_raw = {}
    cashflow_raw = {}
    valuation_raw = {}
    volatility_raw = {}
    momentum_raw = {}
    for code, item in base.items():
        values = finance.get(code)
        finance_status = (
            str((values or {}).get("finance_pit_status") or PIT_DATA_BLOCKED)
        )
        item["finance_pit_status"] = finance_status
        item["finance_pit_reason"] = str(
            (values or {}).get("finance_pit_reason")
            or "PIT_FINANCE_COVERAGE_UNPROVEN"
        )
        item["finance_manifest_hash"] = (values or {}).get(
            "finance_manifest_hash"
        )
        item["finance_revision_id"] = (values or {}).get(
            "finance_revision_id"
        )
        item["finance_content_hash"] = (values or {}).get(
            "finance_content_hash"
        )
        item["finance_published_at"] = (values or {}).get(
            "finance_published_at"
        )
        item["finance_known_at"] = (values or {}).get("finance_known_at")
        item["finance_report_date"] = (values or {}).get(
            "finance_report_date"
        )
        item["finance_authoritative_empty"] = bool(
            (values or {}).get("finance_authoritative_empty")
        )
        item["finance_coverage_id"] = (values or {}).get(
            "finance_coverage_id"
        )
        item["finance_coverage_response_hash"] = (values or {}).get(
            "finance_coverage_response_hash"
        )
        item["finance_coverage_watermark_hash"] = (values or {}).get(
            "finance_coverage_watermark_hash"
        )
        if not values or finance_status != PIT_AVAILABLE:
            item["finance_data_complete"] = 0.0
            item["finance_missing_count"] = 9.0
            item["finance_missing_fields"] = [
                "finance_pit_data_blocked"
            ]
            continue
        missing_fields = _missing_finance_numeric_fields(values)
        item["finance_data_complete"] = float(
            not missing_fields
        )
        item["finance_missing_count"] = float(
            len(missing_fields)
        )
        item["finance_missing_fields"] = missing_fields
        if code not in strategy_eligible_codes:
            continue
        if all(
            values[key] is not None
            for key in (
                "roe_wtd",
                "gross_margin",
                "net_margin",
                "asset_liab_ratio",
            )
        ):
            quality_raw[code] = (
                float(values["roe_wtd"])
                + float(values["gross_margin"]) * 0.25
                + float(values["net_margin"]) * 0.25
                - float(values["asset_liab_ratio"]) * 0.15
            )
        if all(
            values[key] is not None
            for key in (
                "total_rev_yoy_gr",
                "net_profit_yoy_gr",
            )
        ):
            growth_raw[code] = (
                float(values["total_rev_yoy_gr"])
                + float(values["net_profit_yoy_gr"])
            )
        if all(
            values[key] is not None
            for key in ("oper_cf_ps", "cash_flow_ratio")
        ):
            cashflow_raw[code] = (
                float(values["oper_cf_ps"])
                + float(values["cash_flow_ratio"]) * 0.1
            )
        net_asset = values["net_asset_ps"]
        if net_asset is not None and float(net_asset) > 0:
            valuation_raw[code] = (
                item["price"] / float(net_asset)
            )
        volatility_raw[code] = item["atr_14d_pct"]
        momentum_raw[code] = item["return_60d_pct"]
    percentiles = {
        "quality_percentile": _percentile(quality_raw),
        "growth_percentile": _percentile(growth_raw),
        "cashflow_quality_percentile": _percentile(cashflow_raw),
        "valuation_percentile": _percentile(
            valuation_raw,
            higher_better=False,
        ),
        "volatility_20d_percentile": _percentile(volatility_raw),
        "momentum_60d_percentile": _percentile(momentum_raw),
    }
    for feature, values in percentiles.items():
        for code, value in values.items():
            if math.isfinite(float(value)):
                base[code][feature] = float(value)

    strategy_base = {
        code: item
        for code, item in base.items()
        if code in strategy_eligible_codes
    }
    ranked_codes = diversified_universe_codes(strategy_base, limit=limit)
    for code in sorted(required_code_set):
        if code in base and code not in ranked_codes:
            ranked_codes.append(code)
    return {**state, 'base':base,'market':market,'ranked_codes':ranked_codes}


def finish_feature_universe(state, notices, event_evidence):
    as_of = state['as_of']
    dates = state['dates']
    context_cutoff_at = state['context_cutoff_at']
    limit = state['limit']
    required_code_set = state['required_code_set']
    daily_input_window = state['daily_input_window']
    base = state['base']
    market = state['market']
    strategy_eligible_codes = state['strategy_eligible_codes']
    monitoring_only_reasons = state['monitoring_only_reasons']
    theme_statistics = state['theme_statistics']
    industry_evidence = state['industry_evidence']
    concept_snapshot_date = state['concept_snapshot_date']
    ranked_codes = state['ranked_codes']
    selected = []
    for code in ranked_codes:
        item = base[code]
        item["event_pit_status"] = event_evidence["status_by_code"].get(
            code, PIT_DATA_BLOCKED
        )
        item["event_pit_reason"] = event_evidence["reason_by_code"].get(
            code, "PIT_EVENT_COVERAGE_UNPROVEN"
        )
        item["event_manifest_hash"] = event_evidence["manifest_hash"]
        item["event_revision_ids"] = [
            str(row.get("event_revision_id") or "")
            for row in notices.get(code, [])
            if row.get("event_revision_id")
        ]
        item["event_content_hashes"] = [
            str(row.get("event_content_hash") or "")
            for row in notices.get(code, [])
            if row.get("event_content_hash")
        ]
        event_coverage = dict(
            event_evidence.get("coverage_by_code", {}).get(code) or {}
        )
        item["event_authoritative_empty"] = bool(
            item.get("event_pit_status") == PIT_AVAILABLE
            and not notices.get(code)
            and event_coverage
        )
        item["event_coverage_id"] = event_coverage.get("coverage_id")
        item["event_coverage_response_hash"] = event_coverage.get(
            "coverage_response_hash"
        )
        item["event_coverage_watermark_hash"] = event_coverage.get(
            "coverage_watermark_hash"
        )
        item["pit_reconstruction_mode"] = market.get(
            "pit_reconstruction_mode"
        )
        item["pit_reconstruction_sha256"] = market.get(
            "pit_reconstruction_sha256"
        )
        item["pit_reconstructed_at"] = market.get("pit_reconstructed_at")
        pit_strategy_eligible = all(item.get(field)==PIT_AVAILABLE for field in ENTRY_PIT_STATUS_FIELDS)
        item["pit_strategy_status"] = (
            PIT_AVAILABLE if pit_strategy_eligible else PIT_DATA_BLOCKED
        )
        item["pit_strategy_reason"] = (
            ""
            if pit_strategy_eligible
            else ";".join(
                value
                for value in (
                    str(item.get("finance_pit_reason") or ""),
                    str(item.get("event_pit_reason") or ""),
                    str(item.get("industry_pit_reason") or ""),
                )
                if value
            )
        )
        if not pit_strategy_eligible:
            # Positions still remain in ``required_position_monitor`` for exit
            # risk, but missing PIT facts can never qualify a new entry.
            item["entry_eligible"] = 0.0
        item["market_news_risk_score"] = float(
            market.get("news_risk_score") or 0.0
        )
        item["news_context_evidence_status"] = str(
            market.get("context_evidence_status") or "DATA_BLOCKED"
        )
        item["news_context_strategy_eligible"] = bool(
            market.get("context_strategy_eligible")
        )
        item["news_context_reason"] = str(
            market.get("context_reason")
            or "PIT_NEWS_REVISION_AND_COVERAGE_REQUIRED"
        )
        item["overseas_risk_score"] = float(
            market.get("overseas_risk_score") or 0.0
        )
        item.update(
            _event_features(
                notices.get(code, []),
                as_of=as_of,
                return_5d_pct=item["return_5d_pct"],
                amount_ratio=item["amount_ratio_5_20"],
                distance_ma20_pct=item["distance_ma20_pct"],
            )
        )
        if code not in strategy_eligible_codes:
            _block_entry_candidate_features(
                item,
                reason=monitoring_only_reasons.get(
                    code,
                    "ENTRY_DATA_QUALITY_BLOCKED",
                ),
            )
        selected.append(item)
    snapshot_payload = {
        "as_of": as_of.isoformat(),
        "qmt_daily_input_window": daily_input_window,
        "dates": [item.isoformat() for item in dates],
        "rows": [
            {
                "stock_code": item["stock_code"],
                "price": item["price"],
                "latest_amount": item["latest_amount"],
                "theme_code": item.get("theme_code") or "",
                "feature_hash": hashlib.sha256(
                    json.dumps(
                        _snapshot_feature_payload(item),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                ).hexdigest(),
            }
            for item in selected
        ],
        "concept_snapshot_date": (
            concept_snapshot_date.isoformat()
            if concept_snapshot_date
            else None
        ),
        "pit_reconstruction": {
            "mode": market.get("pit_reconstruction_mode") or "",
            "reconstruction_sha256": market.get(
                "pit_reconstruction_sha256"
            ) or "",
            "reconstructed_at": market.get("pit_reconstructed_at") or "",
            "provenance": market.get("pit_reconstruction_provenance") or {},
        },
        "industry_pit": {
            "status": industry_evidence.get("status"),
            "reason": industry_evidence.get("reason"),
            "snapshot_date": industry_evidence.get("snapshot_date"),
            "source": industry_evidence.get("source"),
            "captured_at": industry_evidence.get("captured_at"),
            "snapshot_hash": industry_evidence.get("snapshot_hash"),
        },
        "finance_pit_manifest_hashes": sorted(
            {
                str(item.get("finance_manifest_hash") or "")
                for item in selected
                if item.get("finance_manifest_hash")
            }
        ),
        "event_pit_manifest_hash": event_evidence.get("manifest_hash"),
        "data_quality": {
            key: market.get(key)
            for key in (
                "market_eligible_stock_count",
                "market_latest_coverage_ratio",
                "market_tradable_coverage_ratio",
                "concept_snapshot_age_days",
                "daily_truth_current",
                "daily_truth_schema",
                "daily_provider",
                "qmt_attestation_current",
                "qmt_attestation_status",
                "qmt_attestation_run_id",
                "qmt_attestation_start_date",
                "qmt_attestation_end_date",
                "qmt_attestation_target_rows",
                "qmt_attestation_qmt_rows",
                "qmt_attestation_matched_rows",
                "qmt_attestation_missing_rows",
                "qmt_attestation_mismatched_rows",
            )
        },
        "market_context": {
            "context_cutoff_at": market.get("context_cutoff_at"),
            "context_hash": market.get("context_hash"),
            "context_model_version": market.get(
                "context_model_version"
            ),
            "context_evidence_status": market.get(
                "context_evidence_status"
            ),
            "context_knowledge_time_column": market.get(
                "context_knowledge_time_column"
            ),
            "context_strategy_eligible": market.get(
                "context_strategy_eligible"
            ),
            "context_reason": market.get("context_reason"),
            "funding_eligible": market.get("funding_eligible"),
            "order_authority": market.get("order_authority"),
            "policy_support_score": market.get(
                "policy_support_score"
            ),
            "news_risk_score": market.get("news_risk_score"),
            "overseas_risk_score": market.get(
                "overseas_risk_score"
            ),
            "context_theme_scores": market.get(
                "context_theme_scores"
            ),
            "context_theme_novelty": market.get(
                "context_theme_novelty"
            ),
        },
    }
    snapshot_hash = hashlib.sha256(
        json.dumps(
            snapshot_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "trade_date": dates[-1],
        "feature_time": datetime.combine(
            dates[-1],
            datetime.min.time(),
        ).replace(hour=15),
        "market_features": market,
        "stocks": selected,
        "data_snapshot_hash": snapshot_hash,
        "source": _daily_source_label(market),
        "concept_snapshot_date": (
            concept_snapshot_date.isoformat()
            if concept_snapshot_date
            else None
        ),
        "industry_pit": snapshot_payload["industry_pit"],
        "event_pit_manifest_hash": event_evidence.get("manifest_hash"),
        "theme_count": len(theme_statistics),
    }
