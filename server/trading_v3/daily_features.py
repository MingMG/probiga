from __future__ import annotations

import hashlib
import json
import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
    _exact_snapshot_contract,
    _snapshot_run_count,
    authorized_industry_fallback,
)
from server.common.pit_facts import (
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    load_event_facts,
    load_finance_facts,
    resolve_common_fact_cutoff,
)

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
    infer_name_theme_memberships,
)

logger = logging.getLogger(__name__)

DERIVED_CHANGE_PCT_PROTOCOL = (
    "NATIVE_CLOSE_DIV_NATIVE_PRE_CLOSE_MINUS_ONE_X100_V1"
)


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
                      AND target_rows > 0
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
            "qmt_attestation_reason": "NO_NONEMPTY_RUN_COVERS_TRADE_DATE",
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
               pre_close, amount
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
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
    # Do not materialize the result as ``list[RowMapping]``.  A normal
    # production universe is roughly 350k bars here; keeping every SQLAlchemy
    # mapping alive while pandas makes a second copy can push the scheduler
    # above its memory high-water mark.  Build bounded tuple-backed chunks and
    # release each database batch before doing feature work.
    chunks: list[pd.DataFrame] = []
    with engine.connect() as connection:
        result = connection.execute(
            statement,
            {"dates": dates, "latest_date": dates[-1]},
        )
        columns = list(result.keys())
        while True:
            rows = result.fetchmany(10_000)
            if not rows:
                break
            chunks.append(
                pd.DataFrame.from_records(rows, columns=columns)
            )
    frame = (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=columns)
    )
    chunks.clear()
    if frame.empty:
        return frame
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


_MONITORING_ONLY_NUMERIC_FIELDS = frozenset(
    {
        "price",
        "latest_low",
        "entry_eligible",
        "required_position_monitor",
        "latest_tradable",
    }
)


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


def _load_industries(
    engine: Engine,
    codes: list[str],
    *,
    as_of: date | None = None,
    decision_at: datetime | None = None,
    include_evidence: bool = False,
) -> (
    dict[str, tuple[str, str]]
    | tuple[dict[str, tuple[str, str]], dict[str, Any]]
):
    if not codes:
        empty_evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_INDUSTRY_EMPTY_SCOPE",
            "snapshot_hash": hashlib.sha256(b"[]").hexdigest(),
            "status_by_code": {},
            "reason_by_code": {},
        }
        return ({}, empty_evidence) if include_evidence else {}
    if as_of is None:
        evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_INDUSTRY_EXACT_DATE_REQUIRED",
            "snapshot_hash": hashlib.sha256(
                b"PIT_INDUSTRY_EXACT_DATE_REQUIRED"
            ).hexdigest(),
            "status_by_code": {code: PIT_DATA_BLOCKED for code in codes},
            "reason_by_code": {
                code: "PIT_INDUSTRY_EXACT_DATE_REQUIRED" for code in codes
            },
        }
        return ({}, evidence) if include_evidence else {}
    cutoff = decision_at or datetime.combine(
        as_of + timedelta(days=1), datetime.min.time()
    )
    result: dict[str, tuple[str, str]] = {}
    reason = ""
    run_payload: dict[str, Any] = {}
    try:
        target = as_of.isoformat()
        fallback_reason = ""
        rows: list[dict[str, Any]] = []
        run: dict[str, Any] | None = None
        try:
            run, rows = _exact_snapshot_contract(
                engine, trade_date=target,
            )
        except IndustrySnapshotNotReady:
            # A target-date run from the wrong provider, or one which has not
            # reached QMT_VALIDATED, is present-but-bad.  It must never be
            # hidden behind the explicitly authorized previous-session row.
            if _snapshot_run_count(engine, trade_date=target):
                reason = "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
            else:
                source_date, fallback_reason = authorized_industry_fallback(
                    engine,
                    trade_date=target,
                )
                if not source_date or not fallback_reason:
                    reason = "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_MISSING"
                else:
                    try:
                        run, rows = _exact_snapshot_contract(
                            engine, trade_date=source_date,
                        )
                    except IndustrySnapshotNotReady:
                        reason = (
                            "PIT_INDUSTRY_PREVIOUS_SESSION_SNAPSHOT_MISSING"
                        )
                        run = None
                        rows = []
                    except IndustrySnapshotIntegrityError as exc:
                        reason = (
                            "PIT_INDUSTRY_SNAPSHOT_INCOMPLETE"
                            if "数不完整" in str(exc)
                            else "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
                        )
                        run = None
                        rows = []
        except IndustrySnapshotIntegrityError as exc:
            reason = (
                "PIT_INDUSTRY_SNAPSHOT_INCOMPLETE"
                if "数不完整" in str(exc)
                else "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
            )

        if run is not None:
            captured_at = datetime.fromisoformat(str(run["captured_at"]))
            if captured_at > cutoff:
                reason = "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
                run = None
                rows = []
        if run is not None:
            expected = int(run["industry_relation_count"])
            actual = len(rows)
            run_payload = {
                "snapshot_date": str(run["trade_date"]),
                "target_snapshot_date": target,
                "source_snapshot_date": str(run["trade_date"]),
                "source": str(run["source"]),
                "capture_mode": str(run["capture_mode"]),
                "fallback_reason": fallback_reason,
                "previous_session_fallback": bool(fallback_reason),
                "captured_at": str(run["captured_at"]),
                "expected_industry_count": int(run["industry_count"]),
                "actual_industry_count": len({
                    str(row.get("industry_code") or "") for row in rows
                }),
                "expected_relation_count": expected,
                "actual_relation_count": actual,
                "industry_hash": str(run["industry_hash"]),
            }
            requested = set(codes)
            for row in rows:
                if str(row.get("industry_type") or "") not in {
                    "L1", "一级行业", "申万一级", "SW2021",
                }:
                    continue
                code = str(row.get("stock_code") or "")[:6]
                if code not in requested:
                    continue
                result.setdefault(
                    code,
                    (
                        str(row.get("industry_code") or ""),
                        str(row.get("industry_name") or ""),
                    ),
                )
    except Exception as exc:
        reason = f"PIT_INDUSTRY_SCHEMA_OR_CHAIN_INVALID:{type(exc).__name__}"
        logger.warning("PIT industry exact-date lookup blocked: %s", exc)
        result = {}
    status_by_code = {
        code: PIT_AVAILABLE if code in result else PIT_DATA_BLOCKED
        for code in codes
    }
    reason_by_code = {
        code: (
            ""
            if code in result
            else (reason or "PIT_INDUSTRY_CODE_NOT_IN_EXACT_SNAPSHOT")
        )
        for code in codes
    }
    snapshot_payload = {
        "schema": "probiga.v3-industry-pit-selection.v1",
        "as_of": as_of.isoformat(),
        "decision_at": cutoff.isoformat(),
        "run": run_payload,
        "memberships": {
            code: list(result[code]) for code in sorted(result)
        },
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
    }
    evidence = {
        "status": (
            PIT_AVAILABLE
            if result and len(result) == len(codes)
            else PIT_DATA_BLOCKED
        ),
        "reason": reason or (
            "" if len(result) == len(codes)
            else "PIT_INDUSTRY_PARTIAL_CODE_COVERAGE"
        ),
        "snapshot_hash": hashlib.sha256(
            json.dumps(
                snapshot_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
        **run_payload,
    }
    return (result, evidence) if include_evidence else result


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
    decision_at: datetime | None,
    fact_cutoff_at: datetime | str | None = None,
) -> dict[str, dict[str, Any]]:
    if not codes:
        return {}
    if decision_at is None:
        return {
            code: {
                "finance_pit_status": PIT_DATA_BLOCKED,
                "finance_pit_reason": "PIT_FINANCE_EXACT_DECISION_TIME_REQUIRED",
                "finance_manifest_hash": hashlib.sha256(
                    f"finance:{as_of}:missing-decision-at".encode("utf-8")
                ).hexdigest(),
            }
            for code in codes
        }
    batch = load_finance_facts(
        engine,
        codes=codes,
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        as_of_date=as_of,
    )
    result: dict[str, dict[str, Any]] = {}
    numeric_fields = (
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
    for code in codes:
        raw = dict(batch.facts.get(code) or {})
        coverage = dict(batch.coverage_by_code.get(code) or {})
        status = batch.status_for(code)
        item: dict[str, Any] = {
            key: _optional_finite_float(raw.get(key)) for key in numeric_fields
        }
        item.update(
            {
                "finance_pit_status": (
                    PIT_AVAILABLE if status == PIT_AVAILABLE else PIT_DATA_BLOCKED
                ),
                "finance_pit_reason": (
                    batch.reason_for(code)
                    or (
                        "" if status == PIT_AVAILABLE
                        else "PIT_FINANCE_COVERAGE_UNPROVEN"
                    )
                ),
                "finance_manifest_hash": batch.manifest_hash,
                "finance_revision_id": raw.get("finance_revision_id"),
                "finance_content_hash": raw.get("finance_content_hash"),
                "finance_published_at": raw.get("finance_published_at"),
                "finance_known_at": raw.get("finance_known_at"),
                "finance_report_date": raw.get("finance_report_date"),
                "finance_fact_cutoff_at": batch.fact_cutoff_at,
                "finance_decision_at": batch.decision_at,
                "finance_authoritative_empty": bool(
                    status == PIT_AVAILABLE and not raw and coverage
                ),
                "finance_coverage_id": coverage.get("coverage_id"),
                "finance_coverage_response_hash": coverage.get(
                    "coverage_response_hash"
                ),
                "finance_coverage_watermark_hash": coverage.get(
                    "coverage_watermark_hash"
                ),
            }
        )
        result[code] = item
    return result


def _load_recent_notices(
    engine: Engine,
    *,
    as_of: date,
    codes: list[str],
    decision_at: datetime | None,
    fact_cutoff_at: datetime | str | None = None,
    include_evidence: bool = False,
) -> (
    dict[str, list[dict[str, Any]]]
    | tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]
):
    if not codes:
        empty_evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_EVENT_EMPTY_SCOPE",
            "manifest_hash": hashlib.sha256(b"[]").hexdigest(),
            "status_by_code": {},
            "reason_by_code": {},
        }
        return ({}, empty_evidence) if include_evidence else {}
    if decision_at is None:
        evidence = {
            "status": PIT_DATA_BLOCKED,
            "reason": "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED",
            "manifest_hash": hashlib.sha256(
                f"event:{as_of}:missing-decision-at".encode("utf-8")
            ).hexdigest(),
            "status_by_code": {code: PIT_DATA_BLOCKED for code in codes},
            "reason_by_code": {
                code: "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED" for code in codes
            },
        }
        result: dict[str, list[dict[str, Any]]] = {}
        return (result, evidence) if include_evidence else result
    batch = load_event_facts(
        engine,
        codes=codes,
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        start_date=as_of - timedelta(days=20),
        end_date=as_of,
        require_qmt_complete_batch=True,
    )
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for code in codes:
        for raw in batch.facts.get(code) or []:
            if len(result[code]) >= 10:
                break
            row = dict(raw)
            published_at = str(row.get("event_published_at") or "")
            try:
                row["notice_date"] = date.fromisoformat(published_at[:10])
            except ValueError:
                row["notice_date"] = None
            result[code].append(row)
    status_by_code = {
        code: (
            PIT_AVAILABLE
            if batch.status_for(code) == PIT_AVAILABLE
            else PIT_DATA_BLOCKED
        )
        for code in codes
    }
    reason_by_code = {
        code: (
            batch.reason_for(code)
            or (
                "" if status_by_code[code] == PIT_AVAILABLE
                else "PIT_EVENT_COVERAGE_UNPROVEN"
            )
        )
        for code in codes
    }
    evidence = {
        "status": (
            PIT_AVAILABLE
            if all(value == PIT_AVAILABLE for value in status_by_code.values())
            else PIT_DATA_BLOCKED
        ),
        "reason": (
            ""
            if all(value == PIT_AVAILABLE for value in status_by_code.values())
            else "PIT_EVENT_PARTIAL_OR_UNVERIFIED_COVERAGE"
        ),
        "manifest_hash": batch.manifest_hash,
        "status_by_code": status_by_code,
        "reason_by_code": reason_by_code,
        "coverage_by_code": {
            code: dict(batch.coverage_by_code.get(code) or {})
            for code in codes
        },
        "fact_cutoff_at": batch.fact_cutoff_at,
        "decision_at": batch.decision_at,
    }
    output = dict(result)
    return (output, evidence) if include_evidence else output


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
    codes = _eligible_daily_history_codes(
        frame,
        expected_trade_date=expected_trade_date,
        required_codes=required_code_set,
    )
    code_set = set(codes)
    industries, industry_evidence = _load_industries(
        primary_engine,
        codes,
        as_of=as_of,
        decision_at=context_cutoff_at,
        include_evidence=True,
    )
    theme_memberships, concept_snapshot_date = _load_theme_memberships(
        primary_engine,
        as_of=as_of,
        codes=codes,
        industries=industries,
    )
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
            allow_legacy_display=False,
        )
    )

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
    )
    for item in base.values():
        item["news_theme_context_score"] = theme_context_score(
            _theme_context_label(item),
            dict(market.get("context_theme_scores") or {}),
        )

    common_cutoff: dict[str, Any] = {
        "status": PIT_DATA_BLOCKED,
        "reason": "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED",
        "fact_cutoff_at": "",
        "receipt_root_hash": "",
    }
    if context_cutoff_at is not None:
        common_cutoff = resolve_common_fact_cutoff(
            primary_engine,
            codes=list(base),
            decision_at=context_cutoff_at,
            finance_start_date="1900-01-01",
            finance_end_date=as_of,
            event_start_date=as_of - timedelta(days=20),
            event_end_date=as_of,
            require_qmt_event_batch=True,
        )
    pit_reader_decision_at = (
        context_cutoff_at
        if common_cutoff.get("status") == PIT_AVAILABLE
        else None
    )
    pit_fact_cutoff_at = common_cutoff.get("fact_cutoff_at") or None
    market["pit_common_cutoff_status"] = common_cutoff.get("status")
    market["pit_common_cutoff_reason"] = common_cutoff.get("reason") or ""
    market["pit_fact_cutoff_at"] = common_cutoff.get("fact_cutoff_at") or ""
    market["pit_decision_at"] = common_cutoff.get("decision_at") or ""
    market["pit_common_receipt_root_hash"] = (
        common_cutoff.get("receipt_root_hash") or ""
    )
    finance = _load_finance(
        primary_engine,
        as_of=as_of,
        codes=list(base),
        decision_at=pit_reader_decision_at,
        fact_cutoff_at=pit_fact_cutoff_at,
    )
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
    notices, event_evidence = _load_recent_notices(
        primary_engine,
        as_of=as_of,
        codes=ranked_codes,
        decision_at=pit_reader_decision_at,
        fact_cutoff_at=pit_fact_cutoff_at,
        include_evidence=True,
    )
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
        pit_strategy_eligible = bool(
            item.get("finance_pit_status") == PIT_AVAILABLE
            and item.get("event_pit_status") == PIT_AVAILABLE
            and item.get("industry_pit_status") == PIT_AVAILABLE
        )
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
