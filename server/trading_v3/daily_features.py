from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from .context import load_asof_context, theme_context_score
from .theme_history import (
    EvidenceStatus,
    MARKET_TIMEZONE,
    ThemeMembershipRecord,
    ThemeMembershipSnapshot,
    resolve_theme_history,
)
from .theme_features import (
    Membership,
    attach_best_theme,
    build_theme_alias_index,
    calculate_theme_statistics,
    diversified_universe_codes,
)


def _safe_pct(current: float, base: float) -> float:
    if not base or not math.isfinite(base):
        return 0.0
    return (current / base - 1.0) * 100.0


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


def _latest_trade_dates(
    engine: Engine,
    *,
    as_of: date,
    count: int,
) -> list[date]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT trade_date
                FROM sm_stock_kline
                WHERE trade_date <= :as_of
                  AND k_type = 1
                ORDER BY trade_date DESC
                LIMIT :count
                """
            ),
            {"as_of": as_of, "count": count},
        ).scalars().all()
    return sorted(rows)


def _qmt_attestation_evidence(
    engine: Engine,
    *,
    trade_date: date,
) -> dict[str, Any]:
    """Return fail-closed row-level QMT evidence for one trading day."""
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT run_id, start_date, end_date, status,
                           target_rows, qmt_rows, matched_rows,
                           missing_qmt_rows, mismatched_rows
                    FROM qmt_kline_attestation_run
                    WHERE start_date <= :trade_date
                      AND end_date >= :trade_date
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ),
                {"trade_date": trade_date},
            ).mappings().first()
    except Exception as exc:
        return {
            "qmt_attestation_current": False,
            "qmt_attestation_status": "UNAVAILABLE",
            "qmt_attestation_reason": type(exc).__name__,
        }
    if not row:
        return {
            "qmt_attestation_current": False,
            "qmt_attestation_status": "MISSING",
            "qmt_attestation_reason": "NO_RUN_COVERS_TRADE_DATE",
        }
    evidence = dict(row)
    target_rows = int(evidence.get("target_rows") or 0)
    matched_rows = int(evidence.get("matched_rows") or 0)
    missing_rows = int(evidence.get("missing_qmt_rows") or 0)
    mismatched_rows = int(evidence.get("mismatched_rows") or 0)
    current = (
        str(evidence.get("status") or "") == "COMPLETED"
        and target_rows > 0
        and matched_rows == target_rows
        and missing_rows == 0
        and mismatched_rows == 0
    )
    return {
        "qmt_attestation_current": current,
        "qmt_attestation_status": str(
            evidence.get("status") or "UNKNOWN"
        ),
        "qmt_attestation_run_id": str(evidence.get("run_id") or ""),
        "qmt_attestation_start_date": str(
            evidence.get("start_date") or ""
        ),
        "qmt_attestation_end_date": str(
            evidence.get("end_date") or ""
        ),
        "qmt_attestation_target_rows": target_rows,
        "qmt_attestation_qmt_rows": int(evidence.get("qmt_rows") or 0),
        "qmt_attestation_matched_rows": matched_rows,
        "qmt_attestation_missing_rows": missing_rows,
        "qmt_attestation_mismatched_rows": mismatched_rows,
    }


def _daily_source_label(market_features: dict[str, Any]) -> str:
    prefix = (
        "QMT_ATTESTED_DAILY_KLINE"
        if bool(market_features.get("qmt_attestation_current"))
        else "UNATTESTED_DAILY_KLINE_DATA_BLOCKED"
    )
    return f"{prefix}_PLUS_ASOF_CONCEPT_FUNDAMENTAL_NOTICE"


def _expected_trade_date(
    primary_engine: Engine,
    *,
    as_of: date,
) -> date:
    try:
        with primary_engine.connect() as connection:
            value = connection.execute(
                text(
                    """
                    SELECT MAX(trade_date)
                    FROM si_trade_calendar
                    WHERE trade_status = 1
                      AND trade_date <= :as_of
                    """
                ),
                {"as_of": as_of},
            ).scalar()
    except Exception as exc:
        raise RuntimeError(
            "TRADE_CALENDAR_NOT_READY: 无法确认应到交易日"
        ) from exc
    if not isinstance(value, date):
        raise RuntimeError(
            "TRADE_CALENDAR_NOT_READY: 没有可用交易日"
        )
    return value


def _load_bars(
    engine: Engine,
    *,
    dates: list[date],
) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    statement = text(
        """
        SELECT stock_code,
               CASE WHEN trade_date = :latest_date
                    THEN short_name ELSE '' END AS short_name,
               trade_date, open, close, high, low,
               pre_close, amount, change_pct
        FROM sm_stock_kline
        WHERE k_type = 1
          AND trade_date IN :dates
          AND (
              stock_code LIKE '00%%'
              OR stock_code LIKE '30%%'
              OR stock_code LIKE '60%%'
              OR stock_code LIKE '68%%'
              OR stock_code LIKE '92%%'
          )
        ORDER BY stock_code, trade_date
        """
    ).bindparams(bindparam("dates", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {"dates": dates, "latest_date": dates[-1]},
        ).mappings().all()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    numeric = (
        "open",
        "close",
        "high",
        "low",
        "pre_close",
        "amount",
        "change_pct",
    )
    for column in numeric:
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )
    frame = frame.loc[
        frame[["open", "close", "high", "low"]]
        .gt(0)
        .all(axis=1)
    ].copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str[:6]
    for column in ("open", "close", "high", "low"):
        frame["raw_" + column] = frame[column]
    daily_return = frame["change_pct"] / 100.0
    fallback = frame["close"] / frame["pre_close"] - 1.0
    daily_return = daily_return.where(
        daily_return.notna(),
        fallback,
    ).replace([math.inf, -math.inf], math.nan).fillna(0.0)
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
            "raw_open",
            "raw_high",
        ],
        inplace=True,
        errors="ignore",
    )
    return frame


def _load_industries(
    engine: Engine,
    codes: list[str],
    *,
    as_of: date | None = None,
) -> dict[str, tuple[str, str]]:
    if not codes:
        return {}
    statement = text(
        """
        SELECT stock_code, sw_code, industry_name
        FROM si_industry_sw
        WHERE stock_code IN :codes
          AND (
              :as_of_exclusive IS NULL
              OR etl_sync_at < :as_of_exclusive
          )
          AND industry_type IN ('L1', '一级行业', '申万一级', 'SW2021')
        ORDER BY stock_code, etl_sync_at DESC, id DESC
        """
    ).bindparams(bindparam("codes", expanding=True))
    result: dict[str, tuple[str, str]] = {}
    with engine.connect() as connection:
        for row in connection.execute(
            statement,
            {
                "codes": codes,
                "as_of_exclusive": (
                    datetime.combine(
                        as_of + timedelta(days=1),
                        datetime.min.time(),
                    )
                    if as_of is not None
                    else None
                ),
            },
        ).mappings():
            code = str(row["stock_code"])[:6]
            result.setdefault(
                code,
                (
                    str(row.get("sw_code") or ""),
                    str(row.get("industry_name") or ""),
                ),
            )
    return result


def _load_theme_memberships(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
    industries: dict[str, tuple[str, str]],
) -> tuple[dict[str, list[Membership]], date | None]:
    """Load true as-of QMT concepts and retain SW L1 as a fallback.

    Concept membership is used only when a captured snapshot exists on or
    before the decision date.  Current constituent tables are deliberately
    not used to backfill older dates.
    """

    result: dict[str, list[Membership]] = defaultdict(list)
    for code in codes:
        industry_code, industry_name = industries.get(code, ("", ""))
        if industry_code:
            result[code].append(
                (industry_code, industry_name, "industry")
            )
    snapshot_date: date | None = None
    try:
        with engine.connect() as connection:
            as_of_exclusive = datetime.combine(
                as_of + timedelta(days=1),
                datetime.min.time(),
            )
            run = connection.execute(
                text(
                    """
                    SELECT snapshot_date, source, captured_at,
                           concept_relation_count
                    FROM qmt_membership_snapshot_run
                    WHERE snapshot_date <= :as_of
                      AND quality_status = 'QMT_VALIDATED'
                      AND captured_at < :as_of_exclusive
                    ORDER BY snapshot_date DESC, captured_at DESC, source
                    LIMIT 1
                    """
                ),
                {
                    "as_of": as_of,
                    "as_of_exclusive": as_of_exclusive,
                },
            ).mappings().first()
            if not run:
                return dict(result), None
            snapshot_date = run.get("snapshot_date")
            if isinstance(snapshot_date, str):
                snapshot_date = date.fromisoformat(snapshot_date[:10])
            if not isinstance(snapshot_date, date):
                return dict(result), None
            source = str(run.get("source") or "").strip()
            captured_at = run.get("captured_at")
            if isinstance(captured_at, str):
                captured_at = datetime.fromisoformat(captured_at)
            expected_relations = int(run.get("concept_relation_count") or 0)
            if not source or not isinstance(captured_at, datetime):
                return dict(result), None
            relation_evidence = connection.execute(
                text(
                    """
                    SELECT COUNT(*) AS relation_count,
                           MAX(captured_at) AS last_captured_at
                    FROM qmt_concept_member_snapshot
                    WHERE snapshot_date = :snapshot_date
                      AND source = :source
                      AND quality_status = 'QMT_VALIDATED'
                    """
                ),
                {
                    "snapshot_date": snapshot_date,
                    "source": source,
                },
            ).mappings().one()
            actual_relations = int(
                relation_evidence.get("relation_count") or 0
            )
            last_captured_at = relation_evidence.get("last_captured_at")
            if isinstance(last_captured_at, str):
                last_captured_at = datetime.fromisoformat(last_captured_at)
            if (
                expected_relations <= 0
                or actual_relations != expected_relations
                or not isinstance(last_captured_at, datetime)
                or last_captured_at >= as_of_exclusive
            ):
                return dict(result), None
            statement = text(
                """
                SELECT stock_code, concept_code, concept_name, source
                FROM qmt_concept_member_snapshot
                WHERE snapshot_date = :snapshot_date
                  AND source = :source
                  AND stock_code IN :codes
                  AND quality_status = 'QMT_VALIDATED'
                  AND captured_at < :as_of_exclusive
                ORDER BY stock_code, concept_code
                """
            ).bindparams(bindparam("codes", expanding=True))
            rows = connection.execute(
                statement,
                {
                    "snapshot_date": snapshot_date,
                    "source": source,
                    "codes": codes,
                    "as_of_exclusive": as_of_exclusive,
                },
            ).mappings().all()
    except Exception:
        # The industry fallback remains deterministic on installations that
        # have not yet migrated the QMT snapshot tables.
        return dict(result), None
    recorded_at = max(captured_at, last_captured_at)
    if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
        recorded_at = recorded_at.replace(tzinfo=MARKET_TIMEZONE)
    else:
        recorded_at = recorded_at.astimezone(MARKET_TIMEZONE)
    history_view = resolve_theme_history(
        signal_date=as_of,
        membership_snapshots=(
            ThemeMembershipSnapshot(
                snapshot_id=(
                    f"{source}:{snapshot_date.isoformat()}:"
                    f"{recorded_at.isoformat()}"
                ),
                as_of=snapshot_date,
                recorded_at=recorded_at,
                is_complete=True,
                records=tuple(
                    ThemeMembershipRecord(
                        stock_code=str(row["stock_code"])[:6],
                        theme_code=str(row.get("concept_code") or "").strip(),
                        theme_name=str(
                            row.get("concept_name")
                            or row.get("concept_code")
                            or ""
                        ).strip(),
                        source="concept",
                        effective_from=snapshot_date,
                        effective_to=None,
                    )
                    for row in rows
                ),
            ),
        ),
        news_snapshots=(),
        membership_max_age_days=None,
    )
    if history_view.membership_evidence_status is not EvidenceStatus.AVAILABLE:
        return dict(result), None
    seen: set[tuple[str, str]] = set()
    non_actionable_fragments = (
        "融资融券",
        "转融券",
        "富时罗素",
        "MSCI",
        "标普道琼斯",
        "沪股通",
        "深股通",
        "机构重仓",
        "基金重仓",
        "社保重仓",
        "证金汇金",
        "QFII",
        "低价",
        "高价",
        "预盈预增",
    )
    for code, memberships in history_view.memberships.items():
        for concept_code, concept_name, membership_source in memberships:
            if membership_source != "concept":
                continue
            if not concept_code or (code, concept_code) in seen:
                continue
            if any(
                fragment.lower() in concept_name.lower()
                for fragment in non_actionable_fragments
            ):
                continue
            seen.add((code, concept_code))
            result[code].append(
                (
                    concept_code,
                    concept_name,
                    "concept",
                )
            )
    return dict(result), snapshot_date


def _load_finance(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
) -> dict[str, dict[str, float | None]]:
    if not codes:
        return {}
    statement = text(
        """
        SELECT f.stock_code, f.net_asset_ps, f.oper_cf_ps,
               f.total_rev_yoy_gr, f.net_profit_yoy_gr,
               f.roe_wtd, f.gross_margin, f.net_margin,
               f.cash_flow_ratio, f.asset_liab_ratio
        FROM si_stock_finance f
        JOIN (
            SELECT stock_code, MAX(report_date) AS report_date
            FROM si_stock_finance
            WHERE notice_date <= :as_of
              AND report_date <= :as_of
              AND notice_date >= report_date
              AND stock_code IN :codes
            GROUP BY stock_code
        ) latest
          ON latest.stock_code = f.stock_code
         AND latest.report_date = f.report_date
        WHERE f.notice_date <= :as_of
          AND f.report_date <= :as_of
          AND f.notice_date >= f.report_date
        ORDER BY f.stock_code, f.notice_date DESC, f.id DESC
        """
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {"as_of": as_of, "codes": codes},
        ).mappings().all()
    result = {}
    for row in rows:
        code = str(row["stock_code"])[:6]
        if code in result:
            continue
        result[code] = {
            key: float(row[key]) if row[key] is not None else None
            for key in (
                "net_asset_ps",
                "oper_cf_ps",
                "total_rev_yoy_gr",
                "net_profit_yoy_gr",
                "roe_wtd",
                "gross_margin",
                "net_margin",
                "cash_flow_ratio",
                "asset_liab_ratio",
            )
        }
    return result


def _load_recent_notices(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not codes:
        return {}
    statement = text(
        """
        SELECT stock_code, notice_date, title, column_name
        FROM si_notice_eastmoney
        WHERE notice_date BETWEEN :start_date AND :as_of
          AND stock_code IN :codes
        ORDER BY stock_code, notice_date DESC, id DESC
        """
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {
                "start_date": as_of - timedelta(days=20),
                "as_of": as_of,
                "codes": codes,
            },
        ).mappings().all()
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        code = str(row["stock_code"])[:6]
        if len(result[code]) < 10:
            result[code].append(dict(row))
    return result


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


def load_daily_feature_universe(
    primary_engine: Engine,
    kline_engine: Engine,
    *,
    as_of: date,
    context_cutoff_at: datetime | None = None,
    limit: int = 5000,
    required_codes: Iterable[str] = (),
) -> dict[str, Any]:
    required_code_set = {
        str(code).zfill(6)
        for code in required_codes
        if str(code)
    }
    expected_trade_date = _expected_trade_date(
        primary_engine,
        as_of=as_of,
    )
    dates = _latest_trade_dates(kline_engine, as_of=as_of, count=70)
    if not dates or dates[-1] != expected_trade_date:
        actual = dates[-1].isoformat() if dates else "无"
        raise RuntimeError(
            "QMT_DAILY_KLINE_NOT_READY: "
            f"应到 {expected_trade_date.isoformat()}，实际仅到 {actual}"
        )
    frame = _load_bars(kline_engine, dates=dates)
    if frame.empty or len(dates) < 65:
        raise RuntimeError("至少需要 65 个已收盘交易日的日 K 数据")
    codes = sorted(
        str(code)
        for code, count in frame["stock_code"].value_counts().items()
        if int(count) >= 65
    )
    industries = _load_industries(
        primary_engine,
        codes,
        as_of=as_of,
    )
    theme_memberships, concept_snapshot_date = _load_theme_memberships(
        primary_engine,
        as_of=as_of,
        codes=codes,
        industries=industries,
    )
    market = _market_features(frame, dates, industries)
    market.update(
        _qmt_attestation_evidence(
            kline_engine,
            trade_date=expected_trade_date,
        )
    )
    market["concept_snapshot_age_days"] = (
        float((as_of - concept_snapshot_date).days)
        if concept_snapshot_date is not None
        else None
    )
    market.update(
        load_asof_context(
            primary_engine,
            as_of=as_of,
            cutoff_at=context_cutoff_at,
            theme_aliases=build_theme_alias_index(theme_memberships),
        )
    )

    base: dict[str, dict[str, Any]] = {}
    for raw_code, group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        if len(group) < 65:
            continue
        code = str(raw_code)
        name = str(group.iloc[-1]["short_name"] or "")
        restricted_name = "ST" in name.upper() or "退" in name
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
            close.iloc[-1] < 2
            or amount.iloc[-1] <= 0
            or amount.iloc[-20:].mean() < 50_000_000
        )
        if entry_data_blocked and code not in required_code_set:
            continue
        ma20 = float(close.iloc[-20:].mean())
        ma20_prior = float(close.iloc[-25:-5].mean())
        ma5 = float(close.iloc[-5:].mean())
        ma60 = float(close.iloc[-60:].mean())
        latest_close = float(close.iloc[-1])
        latest_low = float(low.iloc[-1])
        ret5 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-6]))
        ret2 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-3]))
        ret20 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-21]))
        ret60 = _safe_pct(float(close.iloc[-1]), float(close.iloc[-61]))
        average_amount_20d = float(amount.iloc[-20:].mean())
        latest_amount = float(group.iloc[-1]["amount"] or 0)
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
        base[code] = {
            "stock_code": code,
            "stock_name": name,
            "latest_trade_date": pd.Timestamp(
                group.iloc[-1]["trade_date"]
            ).date().isoformat(),
            "price": float(group.iloc[-1]["raw_close"]),
            "latest_low": float(group.iloc[-1]["raw_low"]),
            "entry_eligible": float(
                not restricted_name and not entry_data_blocked
            ),
            "required_position_monitor": float(
                code in required_code_set
            ),
            "latest_tradable": float(amount.iloc[-1] > 0),
            "theme_code": industry_code,
            "theme_name": industry_name,
            "return_2d_pct": ret2,
            "return_5d_pct": ret5,
            "return_20d_pct": ret20,
            "return_60d_pct": ret60,
            "ma20_slope_5d_pct": _safe_pct(ma20, ma20_prior),
            "breakout_20d_proximity": min(
                1.0,
                float(close.iloc[-1]) / max(float(high.iloc[-20:].max()), 1e-9),
            ),
            "amount_ratio_5_20": (
                float(amount.iloc[-5:].mean())
                / max(float(amount.iloc[-20:].mean()), 1.0)
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
            "distance_ma20_pct": _safe_pct(
                latest_close,
                ma20,
            ),
            "distance_ma5_pct": _safe_pct(latest_close, ma5),
            "drawdown_20d_pct": _safe_pct(
                latest_close,
                float(high.iloc[-20:].max()),
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
    }
    theme_statistics = calculate_theme_statistics(
        frame,
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
    )

    finance = _load_finance(
        primary_engine,
        as_of=as_of,
        codes=list(base),
    )
    quality_raw = {}
    growth_raw = {}
    cashflow_raw = {}
    valuation_raw = {}
    volatility_raw = {}
    momentum_raw = {}
    for code, item in base.items():
        values = finance.get(code)
        if not values:
            item["finance_data_complete"] = 0.0
            item["finance_missing_count"] = 9.0
            item["finance_missing_fields"] = [
                "finance_row_missing"
            ]
            continue
        missing_fields = [
            key
            for key, value in values.items()
            if value is None or not math.isfinite(float(value))
        ]
        item["finance_data_complete"] = float(
            not missing_fields
        )
        item["finance_missing_count"] = float(
            len(missing_fields)
        )
        item["finance_missing_fields"] = missing_fields
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

    ranked_codes = diversified_universe_codes(base, limit=limit)
    for code in sorted(required_code_set):
        if code in base and code not in ranked_codes:
            ranked_codes.append(code)
    notices = _load_recent_notices(
        primary_engine,
        as_of=as_of,
        codes=ranked_codes,
    )
    selected = []
    for code in ranked_codes:
        item = base[code]
        item["news_theme_context_score"] = theme_context_score(
            _theme_context_label(item),
            dict(market.get("context_theme_scores") or {}),
        )
        item["market_news_risk_score"] = float(
            market.get("news_risk_score") or 0.0
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
        selected.append(item)
    snapshot_payload = {
        "as_of": as_of.isoformat(),
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
        "data_quality": {
            key: market.get(key)
            for key in (
                "market_eligible_stock_count",
                "market_latest_coverage_ratio",
                "market_tradable_coverage_ratio",
                "concept_snapshot_age_days",
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
        "theme_count": len(theme_statistics),
    }
