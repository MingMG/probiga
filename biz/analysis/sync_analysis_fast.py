# -*- coding: utf-8 -*-
"""
Fast end-of-day analysis and recommendation batch.

The existing deep analysis engine is useful for a small candidate list, but it is
too slow to cover the whole A-share universe every day. This job builds a
complete baseline from synchronized tables, writes stock_analysis_result for all
latest K-line rows, and refreshes st_recommended_stocks with the best ALLOW
candidates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.analysis_output_schema import (
    validate_ai_failure_sample_schema,
    validate_analysis_output_schema,
)
from server.common.recommended_run_history_schema import (
    validate_recommended_run_history_schema,
)
from server.common.daily_stock_universe import (
    load_daily_stock_universe,
    validate_daily_stock_coverage,
)
from server.common.pit_facts import (
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    load_event_facts,
    load_finance_facts,
    normalize_decision_at,
    resolve_common_fact_cutoff,
)
from server.common.analysis_pool_receipt import (
    ANALYSIS_POOL_PUBLISHER_TASK_TYPES,
    build_publication_receipt,
    build_preliminary_upper_subject_receipt,
    build_turnover_evidence,
    build_upper_limit_evidence,
    canonical_sha256,
    is_executable_recommendation,
    read_persisted_pool_manifest,
    research_only_publication_is_safe,
    validate_turnover_evidence,
    validate_preliminary_upper_subject_receipt,
    validate_upper_limit_evidence,
)
from server.common.turnover_snapshot import load_verified_turnover_evidence
from server.common.upper_limit_snapshot import (
    load_latest_verified_upper_limit_evidence,
)
from server.common.chase_risk_policy import (
    CanonicalChaseBar,
    assess_chase_risk,
)

logger = logging.getLogger(__name__)
_SHANGHAI = ZoneInfo("Asia/Shanghai")

_KLINE_FEATURE_DEFAULT_STAGE_TIMEOUT_SECONDS = 300
_KLINE_FEATURE_DEFAULT_QUERY_TIMEOUT_SECONDS = 45
_KLINE_FEATURE_DEFAULT_CHUNK_DAYS = 5

ProgressCallback = Callable[[dict[str, Any]], None]


class KlineFeatureDataBlocked(RuntimeError):
    """Terminal, user-readable failure for the daily K-line feature stage."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _bounded_env_int(
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = str(os.environ.get(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, min(maximum, value))


def _kline_feature_blocked(
    reason_code: str,
    *,
    trade_date: str,
    stage: str,
    detail: str,
) -> KlineFeatureDataBlocked:
    safe_detail = " ".join(str(detail or "").split())[:240]
    return KlineFeatureDataBlocked(
        reason_code,
        (
            f"DATA_BLOCKED: {reason_code}; trade_date={trade_date}; "
            f"stage={stage}; {safe_detail}"
        ),
    )


def _kline_stage_remaining_seconds(
    *,
    deadline: float,
    trade_date: str,
    stage: str,
) -> float:
    remaining = float(deadline) - time.monotonic()
    if remaining <= 0:
        raise _kline_feature_blocked(
            "KLINE_FEATURE_STAGE_TIMEOUT",
            trade_date=trade_date,
            stage=stage,
            detail="90-day K-line feature stage exceeded its bounded runtime",
        )
    return remaining


def _read_kline_feature_frame(
    engine: Engine,
    sql: str,
    *,
    params: dict[str, Any],
    deadline: float,
    trade_date: str,
    stage: str,
    query_timeout_seconds: int,
) -> pd.DataFrame:
    """Run one bounded K-line SELECT and convert transport stalls to evidence."""

    remaining = _kline_stage_remaining_seconds(
        deadline=deadline,
        trade_date=trade_date,
        stage=stage,
    )
    timeout_seconds = max(
        1,
        min(int(query_timeout_seconds), int(math.ceil(remaining))),
    )
    statement = str(sql)
    if getattr(getattr(engine, "dialect", None), "name", "") == "mysql":
        statement = re.sub(
            r"\bSELECT\b",
            f"SELECT /*+ MAX_EXECUTION_TIME({timeout_seconds * 1000}) */",
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
    try:
        frame = pd.read_sql(text(statement), engine, params=params)
    except Exception as exc:
        rendered = " ".join(str(exc).split())
        upper = rendered.upper()
        if (
            "MAX_EXECUTION_TIME" in upper
            or "MAX_STATEMENT_TIME" in upper
            or "QUERY EXECUTION WAS INTERRUPTED" in upper
            or "(3024," in rendered
        ):
            raise _kline_feature_blocked(
                "KLINE_FEATURE_QUERY_TIMEOUT",
                trade_date=trade_date,
                stage=stage,
                detail=(
                    f"one K-line query exceeded {timeout_seconds}s; "
                    "the decision batch was stopped before database saturation"
                ),
            ) from exc
        if "LOST CONNECTION" in upper or "(2013," in rendered:
            raise _kline_feature_blocked(
                "KLINE_FEATURE_QUERY_CONNECTION_LOST",
                trade_date=trade_date,
                stage=stage,
                detail="MySQL connection was lost while reading bounded K-line data",
            ) from exc
        raise
    _kline_stage_remaining_seconds(
        deadline=deadline,
        trade_date=trade_date,
        stage=stage,
    )
    return frame


def _now_shanghai_naive(current: datetime | None = None) -> datetime:
    """Return the production wall clock without leaking the host timezone."""

    value = current or datetime.now(_SHANGHAI)
    if value.tzinfo is None:
        value = value.replace(tzinfo=_SHANGHAI)
    else:
        value = value.astimezone(_SHANGHAI)
    return value.replace(tzinfo=None)


def _formal_analysis_decision_at(value: datetime | str | None) -> datetime:
    """Validate the exact naive Shanghai cutoff carried by a publisher."""

    if isinstance(value, datetime):
        parsed = value
        exact = parsed.tzinfo is None and parsed.microsecond == 0
    else:
        raw = str(value or "").strip()
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise RuntimeError(
                "formal analysis requires an exact Shanghai execution time"
            ) from exc
        exact = (
            parsed.tzinfo is None
            and parsed.microsecond == 0
            and raw in {
                parsed.isoformat(timespec="seconds"),
                parsed.isoformat(sep=" ", timespec="seconds"),
            }
        )
    if not exact:
        raise RuntimeError(
            "formal analysis requires an exact Shanghai execution time"
        )
    return parsed

CRITICAL_NOTICE_KEYWORDS = (
    "退市", "终止上市", "暂停上市", "重大违法", "被立案", "立案调查", "欺诈发行",
)
NEGATIVE_NOTICE_KEYWORDS = (
    "减持", "处罚", "问询函", "监管函", "警示函", "诉讼", "仲裁", "冻结",
    "质押", "亏损", "预亏", "业绩预降", "下修", "债务", "违约", "风险提示",
)
POSITIVE_NOTICE_KEYWORDS = (
    "回购", "增持", "中标", "签订合同", "战略合作", "股权激励", "分红", "利润分配",
    "业绩预增", "扭亏", "订单", "投资者回报",
)


@dataclass(frozen=True)
class BatchStats:
    trade_date: str
    analysis_count: int
    recommendation_count: int
    market_mood_score: float
    flow_date: str
    hot_date: str
    executable_count: int = 0
    canonical_pool_sha256: str = ""
    publication_receipt: dict[str, Any] | None = None


def _analysis_lock_key(database_name: str, trade_date: str) -> str:
    database_digest = hashlib.sha256(
        str(database_name or "").encode("utf-8")
    ).hexdigest()[:16]
    key = f"probiga:analysis-write:{database_digest}:{trade_date}"
    if len(key) > 64:
        raise RuntimeError("analysis execution lock key exceeds MySQL limit")
    return key


@contextmanager
def _analysis_execution_lock(engine: Engine, trade_date: str):
    """Hold one MySQL advisory lock for all recommendation writers that day."""
    normalized_date = str(trade_date or "").strip()[:10]
    if not normalized_date:
        raise ValueError("analysis execution lock requires trade_date")
    dialect_name = str(getattr(getattr(engine, "dialect", None), "name", ""))
    if dialect_name.lower() != "mysql":
        if (
            str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "")
            .strip()
            .lower()
            == "production"
        ):
            raise RuntimeError(
                "production analysis execution requires MySQL advisory lock"
            )
        yield lambda: None
        return

    connection = engine.connect()
    acquired = False
    lock_key = ""
    owner_connection_id: int | None = None
    try:
        database_name = connection.execute(text("SELECT DATABASE()" )).scalar()
        if not database_name:
            raise RuntimeError("analysis advisory lock database identity unavailable")
        lock_key = _analysis_lock_key(str(database_name), normalized_date)
        owner_raw = connection.execute(text("SELECT CONNECTION_ID()" )).scalar()
        try:
            owner_connection_id = int(owner_raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "analysis advisory lock connection identity unavailable"
            ) from exc
        acquired_raw = connection.execute(
            text("SELECT GET_LOCK(:lock_key, 0)"),
            {"lock_key": lock_key},
        ).scalar()
        if acquired_raw is None:
            raise RuntimeError("analysis advisory lock provider returned NULL")
        if int(acquired_raw) != 1:
            raise RuntimeError(
                f"analysis writer already active for {normalized_date}"
            )
        acquired = True

        def _verify_owner() -> None:
            used_by_raw = connection.execute(
                text("SELECT IS_USED_LOCK(:lock_key)"),
                {"lock_key": lock_key},
            ).scalar()
            try:
                used_by = int(used_by_raw)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "analysis advisory lock ownership became unavailable"
                ) from exc
            if used_by != owner_connection_id:
                raise RuntimeError(
                    "analysis advisory lock ownership was lost before write"
                )

        yield _verify_owner
    finally:
        if acquired:
            try:
                released = connection.execute(
                    text("SELECT RELEASE_LOCK(:lock_key)"),
                    {"lock_key": lock_key},
                ).scalar()
                if int(released or 0) != 1:
                    logger.error(
                        "analysis advisory lock release was not acknowledged: %s",
                        lock_key,
                    )
            except Exception:
                logger.exception(
                    "analysis advisory lock release failed: %s",
                    lock_key,
                )
        connection.close()


MODEL_VERSION = "ai-rec-v3-mainwave"

STRATEGY_PROFILES: dict[str, dict[str, Any]] = {
    "ultra_short": {
        "label": "ultra-short",
        "min_score": 68.0,
        "confirm_score": 76.0,
        "max_holding_days": 3,
        "base_position": 4.0,
        "stop_loss_pct": -3.5,
        "take_profit_1_pct": 5.0,
        "take_profit_2_pct": 8.0,
        "cooldown_days": 3,
    },
    "short_term": {
        "label": "short-term",
        "min_score": 68.0,
        "confirm_score": 74.0,
        "max_holding_days": 10,
        "base_position": 6.0,
        "stop_loss_pct": -5.5,
        "take_profit_1_pct": 8.0,
        "take_profit_2_pct": 15.0,
        "cooldown_days": 5,
    },
    "swing": {
        "label": "swing",
        "min_score": 66.0,
        "confirm_score": 72.0,
        "max_holding_days": 30,
        "base_position": 8.0,
        "stop_loss_pct": -8.0,
        "take_profit_1_pct": 15.0,
        "take_profit_2_pct": 30.0,
        "cooldown_days": 10,
    },
    "main_wave": {
        "label": "main-wave",
        "min_score": 70.0,
        "confirm_score": 74.0,
        "max_holding_days": 60,
        "base_position": 7.0,
        "stop_loss_pct": -10.0,
        "take_profit_1_pct": 35.0,
        "take_profit_2_pct": 80.0,
        "cooldown_days": 15,
    },
}


def clamp_score(value: float | int | None, low: float = 0.0, high: float = 100.0) -> float:
    """Clamp a scalar score into 0-100 range."""
    if value is None:
        return 50.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 50.0
    if math.isnan(number) or math.isinf(number):
        return 50.0
    return round(max(low, min(high, number)), 1)


def linear_score(value: float | int | None, low: float, high: float, default: float = 50.0) -> float:
    """Map a scalar value linearly to a 0-100 score."""
    if high == low:
        return clamp_score(default)
    if value is None:
        return clamp_score(default)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return clamp_score(default)
    if math.isnan(number) or math.isinf(number):
        return clamp_score(default)
    return clamp_score((number - low) / (high - low) * 100.0)


def classify_notice_title(title: str | None) -> dict[str, Any]:
    """Classify one notice title for event-risk scoring."""
    title = title or ""
    critical_hits = [kw for kw in CRITICAL_NOTICE_KEYWORDS if kw in title]
    negative_hits = [kw for kw in NEGATIVE_NOTICE_KEYWORDS if kw in title]
    positive_hits = [kw for kw in POSITIVE_NOTICE_KEYWORDS if kw in title]
    return {
        "critical": len(critical_hits),
        "negative": len(negative_hits),
        "positive": len(positive_hits),
        "keywords": critical_hits + negative_hits + positive_hits,
    }


def build_data_quality(row: dict[str, Any], trade_date: str, flow_date: str) -> tuple[float, list[str]]:
    """Score whether one row has enough fresh source data to trust the ranking."""
    flags: list[str] = []
    score = 100.0

    finance_fields = ("roe_wtd", "gross_margin", "net_margin", "asset_liab_ratio")
    finance_pit_status = str(
        row.get("finance_pit_status") or PIT_DATA_BLOCKED
    )
    if finance_pit_status != PIT_AVAILABLE:
        flags.append("pit_finance_data_blocked")
        score -= 35.0
    elif not any(_safe_number(row.get(field), 0.0) for field in finance_fields):
        flags.append("missing_finance")
        score -= 28.0
    if row.get("finance_revision_id"):
        flags.append(f"finance_revision_id={row['finance_revision_id']}")
    if row.get("finance_content_hash"):
        flags.append(f"finance_content_hash={row['finance_content_hash']}")
    if row.get("finance_manifest_hash"):
        flags.append(f"finance_manifest_hash={row['finance_manifest_hash']}")
    if row.get("finance_fact_cutoff_at"):
        flags.append(
            f"pit_fact_cutoff_at={row['finance_fact_cutoff_at']}"
        )
    if row.get("finance_decision_at"):
        flags.append(f"pit_decision_at={row['finance_decision_at']}")
    if row.get("pit_common_receipt_root_hash"):
        flags.append(
            "pit_common_receipt_root_hash="
            f"{row['pit_common_receipt_root_hash']}"
        )
    if row.get("finance_coverage_id"):
        flags.append(f"finance_coverage_id={row['finance_coverage_id']}")
    if row.get("finance_coverage_response_hash"):
        flags.append(
            "finance_coverage_response_hash="
            f"{row['finance_coverage_response_hash']}"
        )
    if row.get("finance_coverage_watermark_hash"):
        flags.append(
            "finance_coverage_watermark_hash="
            f"{row['finance_coverage_watermark_hash']}"
        )

    flow_trade_date_value = row.get("flow_trade_date")
    if isinstance(flow_trade_date_value, datetime):
        row_flow_date = flow_trade_date_value.date().isoformat()
    elif isinstance(flow_trade_date_value, date):
        row_flow_date = flow_trade_date_value.isoformat()
    else:
        row_flow_date = str(flow_trade_date_value or "").strip()[:10]
        if row_flow_date.lower() in {"nan", "nat", "none"}:
            row_flow_date = ""
    if not row_flow_date:
        flags.append("missing_flow")
        score -= 24.0
    elif row_flow_date != trade_date:
        flags.append("stale_flow")
        score -= 12.0

    fused_rank = row.get("fused_rank")
    if fused_rank is None or pd.isna(fused_rank):
        flags.append("missing_hot_rank")
        score -= 10.0

    industry_name = str(row.get("industry_name") or "").strip()
    industry_pit_status = str(
        row.get("industry_pit_status") or PIT_DATA_BLOCKED
    )
    if industry_pit_status != PIT_AVAILABLE or not industry_name:
        flags.append("pit_industry_data_blocked")
        score -= 12.0
    if row.get("industry_snapshot_date"):
        flags.append(
            f"industry_snapshot_date={row['industry_snapshot_date']}"
        )
    if row.get("industry_snapshot_source"):
        flags.append(
            f"industry_snapshot_source={row['industry_snapshot_source']}"
        )
    if row.get("industry_source_snapshot_date"):
        flags.append(
            "industry_source_snapshot_date="
            f"{row['industry_source_snapshot_date']}"
        )
    if bool(row.get("industry_previous_session_fallback")):
        flags.append("industry_previous_open_session_carry_forward=true")
        if row.get("industry_fallback_reason"):
            flags.append(
                f"industry_fallback_reason={row['industry_fallback_reason']}"
            )

    event_pit_status = str(row.get("event_pit_status") or PIT_DATA_BLOCKED)
    if event_pit_status != PIT_AVAILABLE:
        flags.append("pit_event_data_blocked")
        score -= 12.0
    elif row.get("latest_notice_date") is None and int(_safe_number(row.get("notice_count"), 0.0)) == 0:
        flags.append("missing_notice_context")
        score -= 4.0
    if row.get("event_manifest_hash"):
        flags.append(f"event_manifest_hash={row['event_manifest_hash']}")
    if row.get("event_coverage_id"):
        flags.append(f"event_coverage_id={row['event_coverage_id']}")
    if row.get("event_coverage_response_hash"):
        flags.append(
            "event_coverage_response_hash="
            f"{row['event_coverage_response_hash']}"
        )
    if row.get("event_coverage_watermark_hash"):
        flags.append(
            "event_coverage_watermark_hash="
            f"{row['event_coverage_watermark_hash']}"
        )

    return clamp_score(score), flags


def choose_recommend_status(
    stock_code: str,
    short_name: str,
    ai_score: float,
    short_term_score: float,
    long_term_score: float,
    event_risk_level: str,
    amount: float | None,
    change_pct: float | None,
    min_score: float,
    data_quality_score: float = 100.0,
    data_quality_flags: list[str] | None = None,
) -> tuple[str, str]:
    """Gate recommendation eligibility. It is intentionally conservative."""
    name = short_name or ""
    amount = float(amount or 0)
    change_pct = float(change_pct or 0)
    code = str(stock_code).zfill(6)
    flags = set(data_quality_flags or [])

    if "ST" in name.upper() or "退" in name:
        return "BLOCK", "ST或退市风险标的，不进入推荐池"
    if not code.startswith(("0", "3", "6")):
        return "BLOCK", "非沪深A股主代码，不进入推荐池"
    if event_risk_level == "CRITICAL":
        return "BLOCK", "公告存在重大事件风险"
    if "missing_flow" in flags:
        return "SUSPENDED", "缺少目标交易日资金流，暂不进入推荐池"
    if "stale_flow" in flags:
        return "SUSPENDED", "资金流不是目标交易日，暂不进入推荐池"
    if (
        "missing_finance" in flags
        or "pit_finance_data_blocked" in flags
        or "pit_event_data_blocked" in flags
        or "pit_industry_data_blocked" in flags
    ):
        return "SUSPENDED", "关键数据缺失，财务或资金流不完整"
    if data_quality_score < 70:
        return "SUSPENDED", f"数据质量分为{data_quality_score:.1f}，暂不进入推荐池"
    if short_term_score < 40 or long_term_score < 35:
        return "BLOCK", "基础评分过低"
    if event_risk_level == "HIGH":
        return "SUSPENDED", "公告风险较高，等待风险消化"
    if ai_score < min_score:
        return "SUSPENDED", "综合评分未达到推荐阈值"
    if amount < 30_000_000:
        return "SUSPENDED", "成交额偏低，流动性不足"
    if change_pct >= 9.7:
        return "SUSPENDED", "当日涨幅过高，避免追高"
    return "ALLOW", "基础评分通过，仍需盘中量价确认"


def _series_score(values: pd.Series, low: float, high: float, default: float = 50.0) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    if high == low:
        return pd.Series(default, index=values.index, dtype="float64")
    scores = (values - low) / (high - low) * 100.0
    return scores.clip(0, 100).fillna(default)


def _percentile_score(values: pd.Series, default: float = 50.0, ascending: bool = True) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    if values.notna().sum() < 2:
        return pd.Series(default, index=values.index, dtype="float64")
    ranks = values.rank(pct=True, ascending=ascending) * 100.0
    return ranks.fillna(default)


def _round_score(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").clip(0, 100).round(1)


def _safe_number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _valid_price(value: Any) -> bool:
    return _safe_number(value, 0.0) > 0


def _round_price(value: Any) -> float | None:
    number = _safe_number(value, 0.0)
    if number <= 0:
        return None
    return round(number, 2)


def _first_price(*values: Any, default: float = 0.0) -> float:
    for value in values:
        number = _safe_number(value, 0.0)
        if number > 0:
            return number
    return default


def _numeric_col(df: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _ensure_columns(df: pd.DataFrame, defaults: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for column, default in defaults.items():
        if column not in out.columns:
            out[column] = default
    return out


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).fetchall()
    return {str(r[0]) for r in rows}


def latest_trade_date(engine: Engine) -> str:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        """)).scalar()
    if not value:
        raise RuntimeError("sm_stock_kline has no daily K-line data")
    return str(value)[:10]


def _parse_execution_date(execution_time: str | None = None) -> date:
    raw = (execution_time or "").strip()
    if not raw:
        return date.today()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid execution_time: {execution_time}") from exc


def previous_trade_date(engine: Engine, execution_time: str | None = None) -> str:
    """Resolve the strict previous trading day for morning recommendation runs."""
    ref_date = _parse_execution_date(execution_time).isoformat()
    with engine.connect() as conn:
        has_calendar = bool(conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'si_trade_calendar'
        """)).scalar())
        if has_calendar:
            cal_columns = _table_columns(engine, "si_trade_calendar")
            status_filter = ""
            if "trade_status" in cal_columns:
                status_filter = "AND trade_status = 1"
            elif "is_open" in cal_columns:
                status_filter = "AND is_open = 1"
            value = conn.execute(text(f"""
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_date < :ref_date
                  {status_filter}
            """), {"ref_date": ref_date}).scalar()
            if value:
                return str(value)[:10]
        value = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date < :ref_date
        """), {"ref_date": ref_date}).scalar()
    if not value:
        raise RuntimeError(f"Cannot resolve previous trade date before {ref_date}")
    return str(value)[:10]


def assert_trade_date_ready(engine: Engine, trade_date: str, min_coverage: float = 0.80) -> dict[str, Any]:
    """Fail fast when the target trading day is missing or clearly incomplete."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required")
    min_coverage = max(0.0, min(1.0, float(min_coverage)))
    with engine.connect() as conn:
        latest = conn.execute(text("""
            SELECT MAX(trade_date)
            FROM sm_stock_kline
            WHERE k_type = 1
        """)).scalar()
        kline_count = int(conn.execute(text("""
            SELECT COUNT(DISTINCT stock_code)
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date = :trade_date
        """), {"trade_date": trade_date}).scalar() or 0)
        expected_count = int(conn.execute(text("""
            SELECT COUNT(DISTINCT stock_code)
            FROM si_all_code
            WHERE stock_code REGEXP '^(0|3|6)'
        """)).scalar() or 0)
    latest_s = str(latest)[:10] if latest else ""
    if latest_s and latest_s < trade_date:
        raise RuntimeError(f"K-line latest date is {latest_s}, earlier than required {trade_date}")
    minimum = int(expected_count * min_coverage) if expected_count else 1
    if kline_count < minimum:
        raise RuntimeError(
            f"K-line coverage for {trade_date} is incomplete: {kline_count}/{expected_count or '?'} "
            f"(required >= {minimum})"
        )
    return {
        "trade_date": trade_date,
        "latest_kline_date": latest_s,
        "kline_count": kline_count,
        "expected_count": expected_count,
        "min_coverage": min_coverage,
    }


def _tail_text(value: str | None, limit: int = 4000) -> str:
    text_value = value or ""
    if len(text_value) <= limit:
        return text_value
    return text_value[-limit:]


def repair_missing_qmt_kline_for_trade_date(
    trade_date: str,
    progress_callback: ProgressCallback | None = None,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    """Fetch one full-market daily K-line date from Guojin QMT before strict recommendation runs."""
    trade_date = str(trade_date or "").strip()[:10]
    if not trade_date:
        raise ValueError("trade_date is required for QMT K-line repair")

    cmd = [
        sys.executable,
        "-m",
        "biz.stock_market.sync_stock_market",
        "--only",
        "stock_kline",
        "--kline-source",
        "qmt",
        "--kline-start",
        trade_date,
        "--kline-end",
        trade_date,
        "--kline-incremental",
        "--max-stocks",
        "0",
        "--skip-progress",
    ]
    env = os.environ.copy()
    env["SM_STOCK_KLINE_SOURCE"] = "qmt"
    env["SM_MAX_STOCKS"] = "0"
    env["SM_SKIP_GLOBAL_TRUNCATE"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not existing_pythonpath else f"{ROOT}{os.pathsep}{existing_pythonpath}"

    _emit_progress(
        progress_callback,
        stage="repair_kline_start",
        percent=3,
        step=f"strict data missing; repairing Guojin QMT K-line for {trade_date}",
        trade_date=trade_date,
        command=" ".join(cmd),
    )
    started_at = datetime.now()
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(300, int(timeout_seconds or 7200)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _emit_progress(
            progress_callback,
            stage="repair_kline_failed",
            percent=3,
            step=f"Guojin QMT K-line repair timed out for {trade_date}",
            trade_date=trade_date,
            error=str(exc),
        )
        raise RuntimeError(f"Guojin QMT K-line repair timed out for {trade_date}") from exc

    elapsed = (datetime.now() - started_at).total_seconds()
    payload = {
        "trade_date": trade_date,
        "returncode": completed.returncode,
        "elapsed_seconds": round(elapsed, 1),
        "stdout_tail": _tail_text(completed.stdout),
        "stderr_tail": _tail_text(completed.stderr),
        "command": " ".join(cmd),
    }
    if completed.returncode != 0:
        _emit_progress(
            progress_callback,
            stage="repair_kline_failed",
            percent=3,
            step=f"Guojin QMT K-line repair failed for {trade_date}",
            trade_date=trade_date,
            returncode=completed.returncode,
            error=_tail_text(completed.stderr or completed.stdout, 1200),
        )
        raise RuntimeError(
            f"Guojin QMT K-line repair failed for {trade_date}, returncode={completed.returncode}: "
            f"{_tail_text(completed.stderr or completed.stdout, 1200)}"
        )

    _emit_progress(
        progress_callback,
        stage="repair_kline_done",
        percent=6,
        step=f"Guojin QMT K-line repair finished for {trade_date}",
        trade_date=trade_date,
        elapsed_seconds=payload["elapsed_seconds"],
    )
    return payload


def _recent_dates(
    engine: Engine,
    table: str,
    column: str,
    end_date: str,
    limit: int,
    *,
    known_at_column: str | None = None,
    decision_known_at: datetime | str | None = None,
) -> list[str]:
    limit = max(1, int(limit))
    known_clause = ""
    params: dict[str, Any] = {"end_date": end_date}
    if known_at_column is not None:
        if decision_known_at is None:
            raise ValueError("knowledge-aware recent dates require a decision cutoff")
        known_clause = (
            f" AND `{known_at_column}` IS NOT NULL "
            f"AND `{known_at_column}` <= :decision_known_at"
        )
        params["decision_known_at"] = normalize_decision_at(
            decision_known_at
        )
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT `{column}` AS d
            FROM `{table}`
            WHERE `{column}` <= :end_date
              {known_clause}
            ORDER BY `{column}` DESC
            LIMIT {limit}
        """), params).fetchall()
    return [str(r[0])[:10] for r in rows if r[0] is not None]


def _load_canonical_chase_risk_evidence(
    engine: Engine,
    *,
    start_date: str,
    trade_date: str,
    decision_known_at: datetime | str | None = None,
    upper_limit_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    preloaded_bars: pd.DataFrame | None = None,
    stock_codes: list[str] | None = None,
) -> pd.DataFrame:
    """Build the complete conservative V4 execution-risk projection.

    Every consumed bar must be the QMT-attested, supported canonical row.
    Possible limit events are matched against every exchange limit band after
    the 0.01 price-tick rounding rule, conservatively covering unknown ST and
    board classifications without understating V4 risk.
    """

    target = date.fromisoformat(trade_date)
    share_cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(target, datetime.max.time()).replace(microsecond=0)
    )
    if preloaded_bars is not None:
        bars = preloaded_bars.copy()
    else:
        normalized_codes = sorted({
            str(code or "").strip().zfill(6)
            for code in list(stock_codes or [])
            if str(code or "").strip()
        })
        code_params = {
            f"stock_code_{index}": code
            for index, code in enumerate(normalized_codes)
        }
        code_clause = (
            " AND stock_code IN ("
            + ", ".join(f":{key}" for key in code_params)
            + ")"
            if code_params
            else ""
        )
        force_index = (
            " FORCE INDEX (idx_date_ktype)"
            if getattr(getattr(engine, "dialect", None), "name", "") == "mysql"
            else ""
        )
        bars = pd.read_sql(
            text(f"""
            SELECT stock_code, trade_date, `open`, high, low, `close`,
                   volume, amount, pre_close, turnover_ratio,
                   data_source, batch_id, data_version, quality_status,
                   permission_status, received_at
            FROM sm_stock_kline{force_index}
            WHERE k_type=1 AND adjust_type=0
              AND trade_date>=:start_date AND trade_date<=:trade_date
              AND received_at IS NOT NULL
              AND received_at<=:decision_known_at
              {code_clause}
            """),
            engine,
            params={
                "start_date": start_date,
                "trade_date": trade_date,
                "decision_known_at": share_cutoff,
                **code_params,
            },
        )
    columns = (
        "stock_code",
        "turnover_ratio_effective",
        "turnover_evidence_json",
        "upper_limit_evidence_json",
        "chase_evidence_status",
        "chase_effective_streak",
        "chase_recent_peak_streak",
        "chase_sessions_since_peak",
        "chase_cooldown_active",
        "chase_atr14",
        "chase_ma5_extension_atr",
        "chase_extreme_extension",
        "chase_no_capacity",
        "chase_bar_window_root_sha256",
    )
    if bars.empty:
        return pd.DataFrame(columns=columns)
    bars["stock_code"] = (
        bars["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    bars["trade_date"] = pd.to_datetime(
        bars["trade_date"], errors="coerce"
    ).dt.date
    bars["received_at"] = pd.to_datetime(
        bars["received_at"], errors="coerce"
    )
    numeric_columns = (
        "open", "high", "low", "close", "volume", "amount",
        "pre_close", "turnover_ratio",
    )
    historical_numeric_columns = tuple(
        column for column in numeric_columns if column != "turnover_ratio"
    )
    for column in numeric_columns:
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.drop_duplicates(
        ["stock_code", "trade_date"], keep=False
    ).sort_values(["stock_code", "trade_date"])
    turnover_snapshot = load_verified_turnover_evidence(
        engine,
        target_date=target,
        decision_at=share_cutoff,
    )
    # A shares denominator and an older QMT instrument snapshot do not prove
    # target-session turnover: either input may have changed after its last
    # capture.  Until the close collector publishes an immutable, target-date
    # direct-turnover receipt, the producer must not convert those stale facts
    # into a Frozen-V4 PASS.
    upper_snapshot = dict(upper_limit_evidence or {})
    records: list[dict[str, Any]] = []

    for stock_code, raw_group in bars.groupby("stock_code", sort=False):
        # V4 needs 21 observations for return-20/MA20 and only a 10-session
        # cooldown lookback.  Bind exactly that consumed window; older legacy
        # rows must not taint an otherwise fully attested decision input.
        group = raw_group.tail(21).reset_index(drop=True)
        consumed_bar_rows: list[dict[str, Any]] = []
        for bar in group.to_dict(orient="records"):
            numeric_payload: dict[str, Any] = {}
            for field in (
                "open", "high", "low", "close", "volume", "amount",
                "pre_close", "turnover_ratio",
            ):
                raw_value = bar.get(field)
                numeric_payload[field] = (
                    None
                    if raw_value is None or pd.isna(raw_value)
                    else format(Decimal(str(raw_value)).normalize(), "f")
                )
            received = bar.get("received_at")
            received_text = (
                None
                if received is None or pd.isna(received)
                else pd.Timestamp(received).to_pydatetime().isoformat(
                    timespec="microseconds"
                )
            )
            consumed_bar_rows.append({
                "stock_code": stock_code,
                "trade_date": (
                    bar["trade_date"].isoformat()
                    if isinstance(bar.get("trade_date"), date)
                    else str(bar.get("trade_date") or "")
                ),
                **numeric_payload,
                "data_source": str(bar.get("data_source") or ""),
                "batch_id": str(bar.get("batch_id") or ""),
                "data_version": str(bar.get("data_version") or ""),
                "quality_status": str(bar.get("quality_status") or ""),
                "permission_status": str(
                    bar.get("permission_status") or ""
                ),
                "received_at": received_text,
            })
        chase_bar_window_root_sha256 = canonical_sha256({
            "schema": "probiga.analysis-chase-bar-window.v1",
            "stock_code": stock_code,
            "target_date": trade_date,
            "decision_at": share_cutoff.isoformat(timespec="seconds"),
            "rows": consumed_bar_rows,
        })
        latest = group.iloc[-1]
        raw_turnover = latest["turnover_ratio"]
        turnover = Decimal(str(raw_turnover)) if pd.notna(raw_turnover) else None
        turnover_core: dict[str, Any]
        snapshot_row = turnover_snapshot.get(stock_code)
        if snapshot_row is not None:
            turnover = Decimal(str(snapshot_row["turnover_ratio"]))
            if not turnover.is_finite() or turnover < 0:
                raise RuntimeError(
                    f"verified turnover snapshot is invalid for {stock_code}"
                )
            turnover_evidence_json = str(
                snapshot_row["turnover_evidence_json"]
            )
        elif turnover is not None and turnover.is_finite() and turnover >= 0:
            # Historical BigQMT rows contain an unversioned mix of fractional
            # and percentage turnover units.  Preserve the stored fact
            # (NULL-only supplementation) but do not feed it to a percentage-
            # threshold strategy until a producer-side unit contract exists.
            turnover = None
            turnover_core = {
                "status": "DATA_BLOCKED",
                "stock_code": stock_code,
                "trade_date": trade_date,
                "decision_known_at": share_cutoff.isoformat(sep=" "),
                "source_table": "sm_stock_kline",
                "reason": (
                    "DATA_BLOCKED: stored turnover unit is not versioned"
                ),
            }
            turnover_evidence_json = build_turnover_evidence(turnover_core)
        else:
            turnover = None
            turnover_core = {
                "status": "DATA_BLOCKED",
                "stock_code": stock_code,
                "trade_date": trade_date,
                "decision_known_at": share_cutoff.isoformat(sep=" "),
                "source_table": "st_market_field_capture_row",
                "reason": (
                    "DATA_BLOCKED: immutable target-date direct turnover "
                    "snapshot unavailable"
                ),
            }
            turnover_evidence_json = build_turnover_evidence(turnover_core)
        upper_row = upper_snapshot.get(stock_code)
        upper_limits: dict[date, Decimal] = {}
        upper_limit_evidence_json: str
        if upper_row is not None:
            upper_limit_evidence_json = str(
                upper_row.get("upper_limit_evidence_json") or ""
            )
            upper_proof = validate_upper_limit_evidence(
                upper_limit_evidence_json
            )
            if (
                upper_proof.get("status") != "PASS"
                or upper_proof.get("stock_code") != stock_code
                or upper_proof.get("trade_date") != trade_date
            ):
                raise RuntimeError(
                    f"verified upper-limit snapshot identity differs for {stock_code}"
                )
            upper_limits = {
                date.fromisoformat(str(session)): Decimal(str(value))
                for session, value in dict(
                    upper_row.get("upper_limits") or {}
                ).items()
            }
        else:
            upper_limit_evidence_json = build_upper_limit_evidence({
                "status": "DATA_BLOCKED",
                "stock_code": stock_code,
                "trade_date": trade_date,
                "decision_known_at": share_cutoff.isoformat(sep=" "),
                "source_table": "st_market_field_capture_row",
                "reason": (
                    "DATA_BLOCKED: immutable 21-session upper-limit "
                    "snapshot unavailable"
                ),
            })
        core_complete = bool(
            len(group) >= 21
            and latest["trade_date"] == target
            and group[list(historical_numeric_columns)].notna().all(axis=None)
            and turnover is not None
            and (group[["open", "high", "low", "close"]] > 0).all(axis=None)
            and (group["pre_close"] > 0).all()
            and (group[["volume", "amount"]] >= 0).all(axis=None)
            and (group["high"] >= group[["open", "close", "low"]].max(axis=1)).all()
            and (group["low"] <= group[["open", "close", "high"]].min(axis=1)).all()
        )
        source_complete = bool(
            (group["data_source"] == "gj_big_qmt_inner").all()
            and (group["quality_status"] == "QMT_ATTESTED").all()
            and (group["permission_status"] == "SUPPORTED").all()
            and group["received_at"].notna().all()
            and (group["received_at"] <= share_cutoff).all()
            and group["batch_id"].fillna("").astype(str).str.strip().ne("").all()
            and group["data_version"].fillna("").astype(str).str.strip().ne("").all()
        )
        status = "DATA_BLOCKED"
        effective_streak = recent_peak = 0
        sessions_since_peak: int | None = None
        cooldown_active = False
        atr14 = ma5_extension_atr = np.nan
        extreme_extension = False
        no_capacity = True
        group_dates = tuple(group["trade_date"].tolist())
        upper_limit_evidence_complete = bool(
            len(upper_limits) == 21
            and set(upper_limits) == set(group_dates)
            and all(value.is_finite() and value > 0 for value in upper_limits.values())
        )
        if core_complete and source_complete and upper_limit_evidence_complete:
            try:
                upper_proof = validate_upper_limit_evidence(
                    upper_limit_evidence_json
                )
                turnover_proof = validate_turnover_evidence(
                    turnover_evidence_json
                )
                evidence_known_at = max(
                    datetime.fromisoformat(str(upper_proof["captured_at"])),
                    datetime.fromisoformat(str(turnover_proof["captured_at"])),
                )
                source_bars: list[CanonicalChaseBar] = []
                for index, bar in group.iterrows():
                    session = bar["trade_date"]
                    qmt_known_at = pd.Timestamp(
                        bar["received_at"]
                    ).to_pydatetime()
                    knowledge_naive = max(qmt_known_at, evidence_known_at)
                    knowledge_aware = knowledge_naive.replace(tzinfo=_SHANGHAI)
                    source_bars.append(CanonicalChaseBar(
                        record_id=(
                            f"{stock_code}:{session.isoformat()}:"
                            f"{str(bar['batch_id'])}:{str(bar['data_version'])}"
                        ),
                        instrument=stock_code,
                        session=session,
                        knowledge_time=knowledge_aware,
                        open=Decimal(str(bar["open"])),
                        high=Decimal(str(bar["high"])),
                        low=Decimal(str(bar["low"])),
                        close=Decimal(str(bar["close"])),
                        previous_close=Decimal(str(bar["pre_close"])),
                        volume=Decimal(str(bar["volume"])),
                        amount=Decimal(str(bar["amount"])),
                        upper_limit=upper_limits[session],
                        turnover_pct=(
                            turnover if index == len(group) - 1 else None
                        ),
                        suspended=False,
                        quality_status="PASS",
                    ))
                cutoff_aware = share_cutoff.replace(tzinfo=_SHANGHAI)
                assessment = assess_chase_risk(
                    tuple(source_bars),
                    instrument=stock_code,
                    cutoff=cutoff_aware,
                )
                effective_streak = max(
                    assessment.surge_streak,
                    assessment.limit_streak or 0,
                    assessment.recent_peak_streak
                    if assessment.cooldown_active else 0,
                )
                recent_peak = assessment.recent_peak_streak
                sessions_since_peak = assessment.sessions_since_peak
                cooldown_active = assessment.cooldown_active
                atr14 = (
                    float(assessment.atr14)
                    if assessment.atr14 is not None else np.nan
                )
                ma5_extension_atr = (
                    float(assessment.ma5_extension_atr)
                    if assessment.ma5_extension_atr is not None else np.nan
                )
                extreme_extension = assessment.extreme_extension
                no_capacity = assessment.no_capacity
                status = (
                    "ALLOW"
                    if assessment.ordinary_buy_eligible
                    else "BLOCK"
                )
                if (
                    assessment.quality_status != "PASS"
                    or assessment.missing_fields
                ):
                    status = "DATA_BLOCKED"
            except Exception as exc:
                logger.warning(
                    "Frozen V4 evidence blocked one stock %s: %s",
                    stock_code,
                    exc,
                )
                status = "DATA_BLOCKED"
        records.append(
            {
                "stock_code": stock_code,
                "turnover_ratio_effective": (
                    float(turnover) if turnover is not None else np.nan
                ),
                "turnover_evidence_json": turnover_evidence_json,
                "upper_limit_evidence_json": upper_limit_evidence_json,
                "chase_evidence_status": status,
                "chase_effective_streak": effective_streak,
                "chase_recent_peak_streak": recent_peak,
                "chase_sessions_since_peak": sessions_since_peak,
                "chase_cooldown_active": int(cooldown_active),
                "chase_atr14": atr14,
                "chase_ma5_extension_atr": ma5_extension_atr,
                "chase_extreme_extension": int(extreme_extension),
                "chase_no_capacity": int(no_capacity),
                "chase_bar_window_root_sha256": (
                    chase_bar_window_root_sha256
                ),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _attach_canonical_chase_risk_evidence(
    frame: pd.DataFrame,
    engine: Engine,
    *,
    start_date: str,
    trade_date: str,
    decision_known_at: datetime | str | None = None,
    preloaded_bars: pd.DataFrame | None = None,
) -> pd.DataFrame:
    evidence = _load_canonical_chase_risk_evidence(
        engine,
        start_date=start_date,
        trade_date=trade_date,
        decision_known_at=decision_known_at,
        preloaded_bars=preloaded_bars,
    )
    if frame.empty:
        return frame
    out = frame.merge(evidence, on="stock_code", how="left")
    if "turnover_ratio_effective" in out.columns:
        out["turnover_ratio"] = pd.to_numeric(
            out["turnover_ratio_effective"], errors="coerce"
        )
        out = out.drop(columns=["turnover_ratio_effective"])
    return out


def load_kline_features(
    engine: Engine,
    trade_date: str,
    lookback: int = 90,
    *,
    decision_known_at: datetime | str | None = None,
    progress_callback: ProgressCallback | None = None,
) -> pd.DataFrame:
    stage_timeout_seconds = _bounded_env_int(
        "PROBIGA_KLINE_FEATURE_STAGE_TIMEOUT_SECONDS",
        _KLINE_FEATURE_DEFAULT_STAGE_TIMEOUT_SECONDS,
        minimum=30,
        maximum=3600,
    )
    query_timeout_seconds = _bounded_env_int(
        "PROBIGA_KLINE_FEATURE_QUERY_TIMEOUT_SECONDS",
        _KLINE_FEATURE_DEFAULT_QUERY_TIMEOUT_SECONDS,
        minimum=5,
        maximum=300,
    )
    chunk_days = _bounded_env_int(
        "PROBIGA_KLINE_FEATURE_CHUNK_DAYS",
        _KLINE_FEATURE_DEFAULT_CHUNK_DAYS,
        minimum=1,
        maximum=20,
    )
    deadline = time.monotonic() + stage_timeout_seconds
    target_date = date.fromisoformat(trade_date)
    decision_cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(target_date, datetime.max.time()).replace(
            microsecond=0
        )
    )
    dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        lookback,
        known_at_column="received_at",
        decision_known_at=decision_cutoff,
    )
    if not dates:
        raise RuntimeError(f"No K-line dates found before {trade_date}")
    start_date = dates[-1]
    force_index = (
        " FORCE INDEX (idx_date_ktype)"
        if getattr(getattr(engine, "dialect", None), "name", "") == "mysql"
        else ""
    )
    chunk_sql = f"""
        SELECT
          k.stock_code,
          COALESCE(NULLIF(k.short_name, ''), '') AS short_name,
          k.trade_date,
          k.open, k.high, k.low, k.close,
          k.volume, k.amount, k.change_pct, k.turnover_ratio, k.pre_close,
          k.data_source, k.batch_id, k.data_version, k.quality_status,
          k.permission_status, k.received_at
        FROM sm_stock_kline k{force_index}
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date >= :chunk_start_date
          AND k.trade_date <= :chunk_end_date
          AND k.received_at IS NOT NULL
          AND k.received_at <= :decision_known_at
    """
    ordered_dates = sorted(dates)
    date_chunks = [
        ordered_dates[index:index + chunk_days]
        for index in range(0, len(ordered_dates), chunk_days)
    ]
    frames: list[pd.DataFrame] = []
    for index, date_chunk in enumerate(date_chunks, start=1):
        stage = f"date_chunk_{index}_of_{len(date_chunks)}"
        _emit_progress(
            progress_callback,
            stage="load_kline_chunk",
            percent=min(
                12,
                5 + int((index - 1) * 7 / max(1, len(date_chunks))),
            ),
            step=(
                f"分批读取日K特征 {index}/{len(date_chunks)} "
                f"({date_chunk[0]} 至 {date_chunk[-1]})"
            ),
            trade_date=trade_date,
            kline_feature_chunk=index,
            kline_feature_chunk_count=len(date_chunks),
        )
        try:
            frame = _read_kline_feature_frame(
                engine,
                chunk_sql,
                params={
                    "chunk_start_date": date_chunk[0],
                    "chunk_end_date": date_chunk[-1],
                    "decision_known_at": decision_cutoff,
                },
                deadline=deadline,
                trade_date=trade_date,
                stage=stage,
                query_timeout_seconds=query_timeout_seconds,
            )
        except KlineFeatureDataBlocked:
            raise
        except Exception as exc:
            raise _kline_feature_blocked(
                "KLINE_FEATURE_CHUNK_READ_FAILED",
                trade_date=trade_date,
                stage=stage,
                detail=str(exc),
            ) from exc
        if not frame.empty:
            frames.append(frame)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        raise _kline_feature_blocked(
            "KLINE_FEATURE_EMPTY",
            trade_date=trade_date,
            stage="date_chunks_complete",
            detail="bounded 90-day K-line reads returned no rows",
        )
    chase_bars = df.reindex(columns=[
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "amount", "pre_close", "turnover_ratio", "data_source",
        "batch_id", "data_version", "quality_status", "permission_status",
        "received_at",
    ]).copy()

    numeric_cols = [
        "open", "high", "low", "close", "volume", "amount", "change_pct",
        "turnover_ratio", "pre_close",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["short_name"] = df["short_name"].fillna("").astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last")
    df = df.sort_values(["stock_code", "trade_date"])

    grouped = df.groupby("stock_code", group_keys=False)
    for window in (5, 10, 20, 60):
        min_periods = max(3, min(window, window // 2))
        df[f"ma{window}"] = grouped["close"].transform(lambda s, w=window, m=min_periods: s.rolling(w, min_periods=m).mean())
    df["amount_ma5"] = grouped["amount"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["amount_ma20"] = grouped["amount"].transform(lambda s: s.rolling(20, min_periods=8).mean())
    df["pct_5"] = grouped["close"].pct_change(5) * 100.0
    df["pct_20"] = grouped["close"].pct_change(20) * 100.0
    df["volatility_20"] = grouped["change_pct"].transform(lambda s: s.rolling(20, min_periods=8).std())
    df["high_20"] = grouped["high"].transform(lambda s: s.rolling(20, min_periods=8).max())
    df["high_60"] = grouped["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    df["low_60"] = grouped["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    df["drawdown_60"] = (df["close"] / df["high_60"] - 1.0) * 100.0
    df["from_low_60"] = (df["close"] / df["low_60"] - 1.0) * 100.0
    df["dist_ma20"] = (df["close"] / df["ma20"] - 1.0) * 100.0
    df["amount_ratio_5"] = df["amount"] / df["amount_ma5"].replace(0, np.nan)
    df["amount_ratio_20"] = df["amount"] / df["amount_ma20"].replace(0, np.nan)

    target = pd.to_datetime(trade_date).date()
    latest = df[df["trade_date"] == target].copy()
    if latest.empty:
        raise RuntimeError(f"No K-line rows exactly on {trade_date}")
    latest = latest.drop(
        columns=[
            "data_source", "batch_id", "data_version", "quality_status",
            "permission_status", "received_at",
        ],
        errors="ignore",
    )
    result = _attach_canonical_chase_risk_evidence(
        latest.reset_index(drop=True),
        engine,
        start_date=start_date,
        trade_date=trade_date,
        decision_known_at=decision_known_at,
        preloaded_bars=chase_bars,
    )
    _emit_progress(
        progress_callback,
        stage="load_kline_done",
        percent=13,
        step="日K特征分批读取与聚合完成",
        trade_date=trade_date,
        kline_feature_mode="INDEXED_DATE_CHUNKS",
        kline_feature_chunk_count=len(date_chunks),
        kline_feature_rows=len(result),
        elapsed_seconds=round(
            stage_timeout_seconds
            - _kline_stage_remaining_seconds(
                deadline=deadline,
                trade_date=trade_date,
                stage="date_chunks_complete",
            ),
            3,
        ),
    )
    return result


def load_finance(
    engine: Engine,
    trade_date: str,
    *,
    decision_at: datetime | str | None,
    fact_cutoff_at: datetime | str | None = None,
    stock_codes: list[str],
) -> pd.DataFrame:
    """Load strategy finance only from immutable as-known revisions."""

    codes = _normalize_stock_codes(stock_codes) or []
    if not codes:
        return pd.DataFrame({"stock_code": []})
    if decision_at is None:
        return pd.DataFrame(
            [
                {
                    "stock_code": code,
                    "finance_pit_status": PIT_DATA_BLOCKED,
                    "finance_pit_reason": "PIT_FINANCE_EXACT_DECISION_TIME_REQUIRED",
                    "finance_manifest_hash": "",
                }
                for code in codes
            ]
        )
    batch = load_finance_facts(
        engine,
        codes=codes,
        decision_at=decision_at,
        fact_cutoff_at=fact_cutoff_at,
        as_of_date=trade_date,
    )
    rows: list[dict[str, Any]] = []
    for code in codes:
        raw = dict(batch.facts.get(code) or {})
        coverage = dict(batch.coverage_by_code.get(code) or {})
        status = batch.status_for(code)
        item = {
            "stock_code": code,
            **raw,
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
            "finance_covered_through_at": coverage.get(
                "covered_through_at"
            ),
        }
        item["report_date"] = raw.get("finance_report_date")
        rows.append(item)
    df = pd.DataFrame(rows)
    numeric_columns = {
        "basic_eps", "net_asset_ps", "oper_cf_ps", "total_rev_yoy_gr",
        "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr", "roe_wtd",
        "gross_margin", "net_margin", "curr_ratio", "cash_flow_ratio",
        "asset_liab_ratio",
    }
    for column in numeric_columns & set(df.columns):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.drop_duplicates("stock_code", keep="last")


def load_flow_features(
    engine: Engine,
    trade_date: str,
    lookback: int = 25,
    *,
    decision_known_at: datetime | str | None = None,
) -> tuple[pd.DataFrame, str]:
    target = date.fromisoformat(trade_date)
    decision_cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(target, datetime.max.time()).replace(microsecond=0)
    )
    dates = _recent_dates(
        engine,
        "sm_stock_capital_flow_daily",
        "trade_date",
        trade_date,
        lookback,
        known_at_column="etl_sync_at",
        decision_known_at=decision_cutoff,
    )
    if not dates:
        return pd.DataFrame({"stock_code": []}), ""
    start_date = dates[-1]
    flow_date = dates[0]
    sql = """
        SELECT stock_code, trade_date, main_net_inflow, max_net_inflow, lg_net_inflow,
               mid_net_inflow, sm_net_inflow, etl_sync_at
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= :start_date
          AND trade_date <= :trade_date
          AND etl_sync_at IS NOT NULL
          AND etl_sync_at >= TIMESTAMP(trade_date, '15:10:00')
          AND etl_sync_at <= :decision_known_at
        ORDER BY stock_code, trade_date
    """
    df = pd.read_sql(
        text(sql),
        engine,
        params={
            "start_date": start_date,
            "trade_date": trade_date,
            "decision_known_at": decision_cutoff,
        },
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []}), flow_date
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["etl_sync_at"] = pd.to_datetime(df["etl_sync_at"], errors="coerce")
    for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.drop_duplicates(["stock_code", "trade_date"], keep="last").sort_values(["stock_code", "trade_date"])
    grouped = df.groupby("stock_code", group_keys=False)
    df["main_net_inflow_5d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(5, min_periods=1).sum())
    df["main_net_inflow_20d"] = grouped["main_net_inflow"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    latest = df[df["trade_date"] == target].copy()
    latest = latest.rename(columns={"trade_date": "flow_trade_date"})
    close_ready = datetime.combine(target, datetime_time(15, 10))
    if (
        latest.empty
        or latest["etl_sync_at"].isna().any()
        or bool((latest["etl_sync_at"] < close_ready).any())
        or bool((latest["etl_sync_at"] > decision_cutoff).any())
    ):
        raise RuntimeError(
            "DATA_BLOCKED: target-date capital flow is not a post-close PIT fact"
        )
    flow_rows = [
        {
            "stock_code": str(row["stock_code"]),
            "flow_trade_date": row["flow_trade_date"].isoformat(),
            "main_net_inflow": row["main_net_inflow"],
            "max_net_inflow": row["max_net_inflow"],
            "lg_net_inflow": row["lg_net_inflow"],
            "mid_net_inflow": row["mid_net_inflow"],
            "sm_net_inflow": row["sm_net_inflow"],
            "main_net_inflow_5d": row["main_net_inflow_5d"],
            "main_net_inflow_20d": row["main_net_inflow_20d"],
            "etl_sync_at": row["etl_sync_at"].to_pydatetime().isoformat(
                timespec="seconds"
            ),
        }
        for row in latest.sort_values("stock_code").to_dict(orient="records")
    ]
    flow_root = canonical_sha256({
        "schema": "probiga.analysis-eod-flow-input.v1",
        "trade_date": target.isoformat(),
        "rows": flow_rows,
    })
    latest["flow_input_root_sha256"] = flow_root
    latest["flow_input_count"] = len(latest)
    latest["flow_input_min_etl_sync_at"] = latest["etl_sync_at"].min()
    latest["flow_input_max_etl_sync_at"] = latest["etl_sync_at"].max()
    latest["flow_input_decision_at"] = decision_cutoff
    return latest.reset_index(drop=True), flow_date


def validate_exact_daily_flow_coverage(
    engine: Engine,
    *,
    trade_date: str,
    kline: pd.DataFrame,
    flow: pd.DataFrame,
    decision_known_at: datetime | None = None,
) -> dict[str, Any]:
    """Prove exact target-date flow coverage against the immutable universe."""

    universe = load_daily_stock_universe(
        engine,
        trade_date,
        decision_known_at=decision_known_at,
    )
    kline_rows = (
        kline[["stock_code", "volume", "amount"]].to_dict(orient="records")
        if {"stock_code", "volume", "amount"}.issubset(kline.columns)
        else []
    )
    flow_rows = (
        flow[["stock_code"]].to_dict(orient="records")
        if "stock_code" in flow.columns
        else []
    )
    audit = validate_daily_stock_coverage(
        universe,
        kline_rows=kline_rows,
        flow_rows=flow_rows,
    )
    required_proof_columns = {
        "flow_input_root_sha256",
        "flow_input_count",
        "flow_input_min_etl_sync_at",
        "flow_input_max_etl_sync_at",
        "flow_input_decision_at",
    }
    if not required_proof_columns.issubset(flow.columns):
        raise RuntimeError("DATA_BLOCKED: exact capital-flow input proof is absent")
    proof_rows = flow[list(required_proof_columns)].drop_duplicates()
    if len(proof_rows) != 1:
        raise RuntimeError("DATA_BLOCKED: capital-flow input proof is ambiguous")
    proof = proof_rows.iloc[0]
    root = str(proof["flow_input_root_sha256"] or "").strip().lower()
    count = int(proof["flow_input_count"] or 0)
    min_etl = pd.to_datetime(proof["flow_input_min_etl_sync_at"], errors="coerce")
    max_etl = pd.to_datetime(proof["flow_input_max_etl_sync_at"], errors="coerce")
    proof_decision = pd.to_datetime(proof["flow_input_decision_at"], errors="coerce")
    canonical_flow_rows = [
        {
            "stock_code": str(row["stock_code"]).strip().zfill(6),
            "flow_trade_date": str(row["flow_trade_date"])[:10],
            "main_net_inflow": row["main_net_inflow"],
            "max_net_inflow": row["max_net_inflow"],
            "lg_net_inflow": row["lg_net_inflow"],
            "mid_net_inflow": row["mid_net_inflow"],
            "sm_net_inflow": row["sm_net_inflow"],
            "main_net_inflow_5d": row["main_net_inflow_5d"],
            "main_net_inflow_20d": row["main_net_inflow_20d"],
            "etl_sync_at": pd.to_datetime(row["etl_sync_at"]).to_pydatetime().isoformat(
                timespec="seconds"
            ),
        }
        for row in flow.sort_values("stock_code").to_dict(orient="records")
    ]
    calculated_root = canonical_sha256({
        "schema": "probiga.analysis-eod-flow-input.v1",
        "trade_date": trade_date,
        "rows": canonical_flow_rows,
    })
    cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(
            date.fromisoformat(trade_date), datetime.max.time()
        ).replace(microsecond=0)
    )
    close_ready = datetime.combine(
        date.fromisoformat(trade_date), datetime_time(15, 10)
    )
    if (
        re.fullmatch(r"[0-9a-f]{64}", root) is None
        or calculated_root != root
        or count != len(flow)
        or pd.isna(min_etl)
        or pd.isna(max_etl)
        or pd.isna(proof_decision)
        or min_etl.to_pydatetime() < close_ready
        or max_etl.to_pydatetime() > cutoff
        or proof_decision.to_pydatetime() != cutoff
    ):
        raise RuntimeError("DATA_BLOCKED: capital-flow PIT proof differs")
    audit.update({
        "flow_input_root_sha256": root,
        "flow_input_count": count,
        "flow_input_min_etl_sync_at": min_etl.to_pydatetime().isoformat(
            timespec="seconds"
        ),
        "flow_input_max_etl_sync_at": max_etl.to_pydatetime().isoformat(
            timespec="seconds"
        ),
        "flow_input_decision_at": cutoff.isoformat(timespec="seconds"),
    })
    logger.info(
        "Exact capital-flow coverage verified: date=%s expected=%s traded=%s "
        "flow=%s catalog_hash=%s",
        trade_date,
        audit["expected_count"],
        audit["traded_count"],
        audit["flow_count"],
        audit["expected_code_set_hash"],
    )
    return audit


def _verify_full_market_turnover_inputs(
    frame: pd.DataFrame,
    *,
    trade_date: str,
) -> dict[str, Any]:
    """Bind every scored target row to one immutable turnover run/root."""

    if frame.empty or "turnover_evidence_json" not in frame.columns:
        raise RuntimeError(
            "DATA_BLOCKED: full-market turnover evidence is unavailable"
        )
    identities: set[tuple[str, ...]] = set()
    proof_items: list[dict[str, str]] = []
    codes: set[str] = set()
    for source in frame.to_dict(orient="records"):
        code = str(source.get("stock_code") or "").strip().zfill(6)
        if code in codes:
            raise RuntimeError(
                "DATA_BLOCKED: full-market turnover evidence is duplicated"
            )
        codes.add(code)
        try:
            proof = validate_turnover_evidence(
                source.get("turnover_evidence_json")
            )
        except ValueError as exc:
            raise RuntimeError(
                f"DATA_BLOCKED: turnover evidence invalid for {code}: {exc}"
            ) from exc
        if (
            proof.get("status") != "PASS"
            or str(proof.get("stock_code") or "") != code
            or str(proof.get("trade_date") or "") != trade_date
        ):
            raise RuntimeError(
                f"DATA_BLOCKED: turnover evidence incomplete for {code}"
            )
        identity = tuple(
            str(proof.get(field) or "").strip().lower()
            for field in (
                "snapshot_run_id",
                "snapshot_semantic_sha256",
                "authority_proof_identity",
                "authority_proof_sha256",
                "authority_set_sha256",
                "collector_build_sha",
                "collector_binary_sha256",
            )
        )
        if (
            not identity[0]
            or any(
                re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in (identity[1], identity[3], identity[4], identity[6])
            )
            or re.fullmatch(r"[0-9a-f]{40}", identity[5]) is None
        ):
            raise RuntimeError(
                f"DATA_BLOCKED: turnover run identity incomplete for {code}"
            )
        identities.add(identity)
        proof_items.append({
            "stock_code": code,
            "proof_sha256": str(proof.get("proof_sha256") or "").lower(),
        })
    if len(identities) != 1:
        raise RuntimeError("DATA_BLOCKED: turnover inputs span multiple runs")
    identity = next(iter(identities))
    return {
        "turnover_snapshot_run_id": identity[0],
        "turnover_snapshot_semantic_sha256": identity[1],
        "turnover_authority_identity": identity[2],
        "turnover_authority_sha256": identity[3],
        "turnover_authority_set_sha256": identity[4],
        "turnover_collector_build_sha": identity[5],
        "turnover_collector_binary_sha256": identity[6],
        "turnover_full_market_count": len(codes),
        "turnover_full_market_proof_root_sha256": canonical_sha256({
            "schema": "probiga.analysis-turnover-input-set.v1",
            "trade_date": trade_date,
            "proofs": sorted(proof_items, key=lambda item: item["stock_code"]),
        }),
    }


def load_hot_rank(
    engine: Engine,
    trade_date: str,
    *,
    decision_at: datetime | str | None = None,
) -> tuple[pd.DataFrame, str]:
    """Load only exact-date, per-stock multi-source heat evidence.

    The provider feeds behind this table are current-snapshot-only.  Falling
    back to an older partition would silently score historical heat as fresh;
    accepting a one-source row would call an unfused ranking a consensus.
    Both cases degrade to a missing heat factor instead.
    """

    # ``merge_hot_rank`` replaces the mutable same-day partition.  The table
    # therefore cannot answer an as-of query: filtering its current rows by an
    # ETL timestamp would not restore rows deleted by a later replacement.
    # Formal publications always carry ``decision_at`` and must use a neutral,
    # explicitly-missing factor until hot-rank batches are append-only and
    # receipt-bound.
    if decision_at is not None:
        logger.warning(
            "DATA_BLOCKED: mutable hot-rank partition is not PIT-replayable; "
            "formal factor disabled: trade_date=%s decision_at=%s",
            trade_date,
            decision_at,
        )
        return pd.DataFrame({"stock_code": []}), ""

    sql = """
        SELECT stock_code, fused_rank, total_score, source_flag
        FROM st_hot_rank_fused
        WHERE snapshot_date = :trade_date
    """
    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date})
    if df.empty:
        logger.warning(
            "DATA_BLOCKED: exact-date multi-source hot rank is unavailable: "
            "trade_date=%s",
            trade_date,
        )
        return pd.DataFrame({"stock_code": []}), ""
    multi_source_flags = {
        "all",
        "east_ths_xq",
        "east_ths_sina",
        "east_xq_sina",
        "ths_xq_sina",
        "both",
        "east_xq",
        "east_sina",
        "ths_xq",
        "ths_sina",
        "xq_sina",
    }
    df = df[
        df["source_flag"].fillna("").astype(str).str.strip().str.lower().isin(
            multi_source_flags
        )
    ].copy()
    if df.empty:
        logger.warning(
            "DATA_BLOCKED: exact-date hot rank has no per-stock multi-source "
            "consensus: trade_date=%s",
            trade_date,
        )
        return pd.DataFrame({"stock_code": []}), ""
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fused_rank"] = pd.to_numeric(df["fused_rank"], errors="coerce")
    df["hot_total_score"] = pd.to_numeric(df["total_score"], errors="coerce")
    # ``st_hot_rank_fused.industry_name`` is derived from mutable current
    # reference data.  It remains available to display APIs, but may not enter
    # a historical/production recommendation score.  The exact-date QMT
    # membership loader below is the only strategy-authoritative industry.
    df["industry_name"] = ""
    return df.drop_duplicates("stock_code", keep="last"), trade_date


def load_notice_features(
    engine: Engine,
    trade_date: str,
    lookback_days: int = 14,
    *,
    decision_at: datetime | str | None,
    fact_cutoff_at: datetime | str | None = None,
    stock_codes: list[str],
) -> pd.DataFrame:
    codes = _normalize_stock_codes(stock_codes) or []
    if not codes:
        return pd.DataFrame({"stock_code": []})
    if decision_at is None:
        return pd.DataFrame(
            [
                {
                    "stock_code": code,
                    "notice_count": 0,
                    "notice_positive": 0,
                    "notice_negative": 0,
                    "notice_critical": 0,
                    "latest_notice_date": None,
                    "latest_notice_time": None,
                    "risk_titles": [],
                    "positive_titles": [],
                    "event_revision_ids": [],
                    "event_content_hashes": [],
                    "event_pit_status": PIT_DATA_BLOCKED,
                    "event_pit_reason": "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED",
                    "event_manifest_hash": "",
                }
                for code in codes
            ]
        )
    decision = normalize_decision_at(decision_at)
    batch = load_event_facts(
        engine,
        codes=codes,
        decision_at=decision,
        fact_cutoff_at=fact_cutoff_at,
        start_date=date.fromisoformat(trade_date) - timedelta(days=lookback_days),
        end_date=trade_date,
        require_qmt_complete_batch=True,
    )
    records: dict[str, dict[str, Any]] = {}
    for code in codes:
        status = batch.status_for(code)
        coverage = dict(batch.coverage_by_code.get(code) or {})
        rec = {
            "stock_code": code,
            "notice_count": 0,
            "notice_positive": 0,
            "notice_negative": 0,
            "notice_critical": 0,
            "latest_notice_date": None,
            "latest_notice_time": None,
            "risk_titles": [],
            "positive_titles": [],
            "event_revision_ids": [],
            "event_content_hashes": [],
            "event_pit_status": (
                PIT_AVAILABLE if status == PIT_AVAILABLE else PIT_DATA_BLOCKED
            ),
            "event_pit_reason": (
                batch.reason_for(code)
                or (
                    "" if status == PIT_AVAILABLE
                    else "PIT_EVENT_COVERAGE_UNPROVEN"
                )
            ),
            "event_manifest_hash": batch.manifest_hash,
            "event_fact_cutoff_at": batch.fact_cutoff_at,
            "event_decision_at": batch.decision_at,
            "event_authoritative_empty": bool(
                status == PIT_AVAILABLE
                and not (batch.facts.get(code) or [])
                and coverage
            ),
            "event_coverage_id": coverage.get("coverage_id"),
            "event_coverage_response_hash": coverage.get(
                "coverage_response_hash"
            ),
            "event_coverage_watermark_hash": coverage.get(
                "coverage_watermark_hash"
            ),
            "event_covered_through_at": coverage.get("covered_through_at"),
        }
        for row in batch.facts.get(code) or []:
            title = str(row.get("title") or "")
            cls = classify_notice_title(title)
            rec["notice_count"] += 1
            rec["notice_positive"] += cls["positive"]
            rec["notice_negative"] += cls["negative"]
            rec["notice_critical"] += cls["critical"]
            published_at = str(row.get("event_published_at") or "")
            notice_date = published_at[:10] or None
            if published_at and (
                rec["latest_notice_time"] is None
                or published_at > str(rec["latest_notice_time"])
            ):
                rec["latest_notice_time"] = published_at
            if notice_date and (
                rec["latest_notice_date"] is None
                or str(notice_date) > str(rec["latest_notice_date"])
            ):
                rec["latest_notice_date"] = notice_date
            rec["event_revision_ids"].append(row.get("event_revision_id"))
            rec["event_content_hashes"].append(row.get("event_content_hash"))
            if (
                cls["negative"] or cls["critical"]
            ) and len(rec["risk_titles"]) < 3:
                rec["risk_titles"].append(title[:80])
            if cls["positive"] and len(rec["positive_titles"]) < 3:
                rec["positive_titles"].append(title[:80])
        records[code] = rec

    return pd.DataFrame(records.values())


def load_news_features(
    engine: Engine,
    trade_date: str,
    cutoff_time: str | None = None,
    lookback_days: int = 3,
) -> pd.DataFrame:
    try:
        has_news_table = _table_exists(engine, "st_news_flash")
    except Exception:
        has_news_table = False
    if not has_news_table:
        return pd.DataFrame({"stock_code": []})
    cutoff = cutoff_time or f"{trade_date} 23:59:59"
    sql = """
        SELECT title, content, publish_time, stocks
        FROM st_news_flash
        WHERE publish_time >= DATE_SUB(:cutoff_time, INTERVAL :lookback_days DAY)
          AND publish_time <= :cutoff_time
        ORDER BY publish_time DESC
        LIMIT 3000
    """
    df = pd.read_sql(
        text(sql),
        engine,
        params={"cutoff_time": cutoff, "lookback_days": int(lookback_days)},
    )
    if df.empty:
        return pd.DataFrame({"stock_code": []})

    records: dict[str, dict[str, Any]] = {}
    for row in df.to_dict(orient="records"):
        raw_stocks = row.get("stocks")
        try:
            stocks = json.loads(raw_stocks) if isinstance(raw_stocks, str) else (raw_stocks or [])
        except Exception:
            stocks = []
        codes: list[str] = []
        for item in stocks if isinstance(stocks, list) else []:
            if isinstance(item, dict):
                code = str(item.get("code") or item.get("symbol") or "").strip()
            else:
                code = str(item or "").strip()
            digits = "".join(ch for ch in code if ch.isdigit())
            if len(digits) >= 6:
                codes.append(digits[-6:])
        if not codes:
            continue
        title = str(row.get("title") or "")
        content = str(row.get("content") or "")
        cls = classify_notice_title(f"{title} {content}"[:500])
        publish_time = str(row.get("publish_time") or "")[:19]
        for code in sorted(set(codes)):
            rec = records.setdefault(code, {
                "stock_code": code,
                "news_count": 0,
                "news_positive": 0,
                "news_negative": 0,
                "news_critical": 0,
                "latest_news_time": None,
                "news_risk_titles": [],
                "news_positive_titles": [],
            })
            rec["news_count"] += 1
            rec["news_positive"] += cls["positive"]
            rec["news_negative"] += cls["negative"]
            rec["news_critical"] += cls["critical"]
            if publish_time and (rec["latest_news_time"] is None or publish_time > str(rec["latest_news_time"])):
                rec["latest_news_time"] = publish_time
            if (cls["negative"] or cls["critical"]) and len(rec["news_risk_titles"]) < 3:
                rec["news_risk_titles"].append(title[:80] or content[:80])
            if cls["positive"] and len(rec["news_positive_titles"]) < 3:
                rec["news_positive_titles"].append(title[:80] or content[:80])
    return pd.DataFrame(records.values()) if records else pd.DataFrame({"stock_code": []})


def merge_event_features(notices: pd.DataFrame, news: pd.DataFrame) -> pd.DataFrame:
    if notices is None or notices.empty:
        base = pd.DataFrame({"stock_code": []})
    else:
        base = notices.copy()
    if news is None or news.empty or "stock_code" not in news.columns:
        return base
    if base.empty or "stock_code" not in base.columns:
        base = pd.DataFrame({"stock_code": news["stock_code"].astype(str).str.zfill(6).unique()})
    out = base.merge(news, on="stock_code", how="outer")
    def _num_col(name: str) -> pd.Series:
        if name not in out.columns:
            return pd.Series(0, index=out.index, dtype="float64")
        return pd.to_numeric(out[name], errors="coerce").fillna(0)
    for col in ("notice_count", "notice_positive", "notice_negative", "notice_critical"):
        out[col] = _num_col(col)
    out["notice_count"] = out["notice_count"] + _num_col("news_count")
    out["notice_positive"] = out["notice_positive"] + _num_col("news_positive")
    out["notice_negative"] = out["notice_negative"] + _num_col("news_negative")
    out["notice_critical"] = out["notice_critical"] + _num_col("news_critical")
    latest_notice = out.get("latest_notice_date")
    latest_news = out.get("latest_news_time")
    if latest_notice is not None and latest_news is not None:
        out["latest_notice_date"] = latest_notice.fillna("").astype(str).combine(
            latest_news.fillna("").astype(str).str[:10],
            lambda a, b: max(a, b) if a and b else (a or b or None),
        )
    elif latest_news is not None:
        out["latest_notice_date"] = latest_news.fillna("").astype(str).str[:10]
    return out


def compute_market_mood(kline: pd.DataFrame) -> float:
    change = pd.to_numeric(kline.get("change_pct"), errors="coerce")
    if change.empty or change.notna().sum() == 0:
        return 50.0
    advance_ratio = float((change > 0).mean())
    limit_up_ratio = float((change >= 9.7).mean())
    limit_down_ratio = float((change <= -9.7).mean())
    avg_change = float(change.mean())
    score = 50 + (advance_ratio - 0.5) * 70 + avg_change * 4 + (limit_up_ratio - limit_down_ratio) * 120
    return clamp_score(score)


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).scalar()
    return bool(value)


def _validate_learning_tables(engine: Engine) -> None:
    validate_ai_failure_sample_schema(engine)


def load_confidence_features(
    engine: Engine,
    trade_date: str,
    lookback_days: int = 5,
    *,
    decision_at: datetime | str | None = None,
) -> pd.DataFrame:
    # ``stock_analysis_result`` has no immutable publication/knowledge-time
    # receipt.  Backfilled historical rows can otherwise change a replay after
    # its decision cutoff.  Use the deterministic neutral default for formal
    # publications until that history is versioned.
    if decision_at is not None:
        logger.warning(
            "DATA_BLOCKED: mutable confidence history is not PIT-replayable; "
            "formal factor disabled: trade_date=%s decision_at=%s",
            trade_date,
            decision_at,
        )
        return pd.DataFrame({"stock_code": []})
    if not _table_exists(engine, "stock_analysis_result"):
        return pd.DataFrame({"stock_code": []})
    with engine.connect() as conn:
        dates = conn.execute(text(f"""
            SELECT DISTINCT analysis_date
            FROM stock_analysis_result
            WHERE analysis_date < :trade_date
            ORDER BY analysis_date DESC
            LIMIT {max(1, int(lookback_days))}
        """), {"trade_date": trade_date}).fetchall()
    hist_dates = [str(r[0])[:10] for r in dates if r[0] is not None]
    if not hist_dates:
        return pd.DataFrame({"stock_code": []})
    placeholders = ", ".join([f":d{i}" for i in range(len(hist_dates))])
    params = {"trade_date": trade_date, **{f"d{i}": d for i, d in enumerate(hist_dates)}}
    sql = f"""
        SELECT stock_code, analysis_date,
               COALESCE(short_term_score, 0) * 0.58 + COALESCE(long_term_score, 0) * 0.42 AS hist_score
        FROM stock_analysis_result
        WHERE analysis_date IN ({placeholders})
    """
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["hist_score"] = pd.to_numeric(df["hist_score"], errors="coerce")
    grouped = df.groupby("stock_code")["hist_score"]
    out = grouped.agg(["count", "std", "mean"]).reset_index()
    out["score_std_5d"] = pd.to_numeric(out["std"], errors="coerce").fillna(0.0)
    out["confidence_score"] = (100.0 - out["score_std_5d"] * 8.0).clip(35, 100)
    out.loc[out["count"] < 3, "confidence_score"] = 62.0
    return out[["stock_code", "confidence_score", "score_std_5d"]]


def load_recommendation_history(
    engine: Engine,
    trade_date: str,
    lookback_days: int = 30,
    *,
    decision_at: datetime | str | None = None,
) -> pd.DataFrame:
    # The recommendation partition is mutable and its rows do not expose a
    # per-version knowledge timestamp.  Do not let later activation, repair, or
    # backfill change a formal replay's score.
    if decision_at is not None:
        logger.warning(
            "DATA_BLOCKED: mutable recommendation history is not "
            "PIT-replayable; formal factor disabled: trade_date=%s "
            "decision_at=%s",
            trade_date,
            decision_at,
        )
        return pd.DataFrame({"stock_code": []})
    if not _table_exists(engine, "st_recommended_stocks"):
        return pd.DataFrame({"stock_code": []})
    columns = _table_columns(engine, "st_recommended_stocks")
    score_expr = "COALESCE(final_trade_score, ai_score, 0)" if "final_trade_score" in columns else "COALESCE(ai_score, 0)"
    strategy_expr = "COALESCE(primary_strategy, strategy_profile, '')" if "primary_strategy" in columns else "''"
    sql = f"""
        SELECT stock_code,
               MAX(pick_date) AS last_pick_date,
               MAX({score_expr}) AS max_recent_trade_score,
               MAX({strategy_expr}) AS last_strategy
        FROM st_recommended_stocks
        WHERE pick_date < :trade_date
          AND pick_date >= DATE_SUB(:trade_date, INTERVAL :lookback DAY)
        GROUP BY stock_code
    """
    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date, "lookback": int(lookback_days)})
    if df.empty:
        return pd.DataFrame({"stock_code": []})
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["max_recent_trade_score"] = pd.to_numeric(df["max_recent_trade_score"], errors="coerce").fillna(0.0)
    return df


def load_failure_features(
    engine: Engine,
    trade_date: str,
    *,
    decision_at: datetime | str | None = None,
) -> pd.DataFrame:
    # Failure samples and simulated positions are mutable and do not provide a
    # complete knowledge-time lineage.  In particular, a failure entered after
    # the decision cutoff must not downgrade or suppress an earlier signal.
    if decision_at is not None:
        logger.warning(
            "DATA_BLOCKED: mutable failure-learning history is not "
            "PIT-replayable; formal factor disabled: trade_date=%s "
            "decision_at=%s",
            trade_date,
            decision_at,
        )
        return pd.DataFrame({"stock_code": []})
    _validate_learning_tables(engine)
    pieces: list[pd.DataFrame] = []
    if _table_exists(engine, "st_sim_position"):
        try:
            sim = pd.read_sql(text("""
                SELECT stock_code, COUNT(*) AS fail_count
                FROM st_sim_position
                WHERE status = 'closed'
                  AND COALESCE(profit_rate, 0) < 0
                  AND COALESCE(sell_date, buy_date) >= DATE_SUB(:trade_date, INTERVAL 180 DAY)
                GROUP BY stock_code
            """), engine, params={"trade_date": trade_date})
            if not sim.empty:
                pieces.append(sim)
        except Exception:
            pass
    try:
        manual = pd.read_sql(text("""
            SELECT stock_code, COUNT(*) AS fail_count
            FROM st_ai_failure_samples
            WHERE result = 'fail'
              AND (signal_date IS NULL OR signal_date >= DATE_SUB(:trade_date, INTERVAL 180 DAY))
            GROUP BY stock_code
        """), engine, params={"trade_date": trade_date})
        if not manual.empty:
            pieces.append(manual)
    except Exception:
        pass
    if not pieces:
        return pd.DataFrame({"stock_code": []})
    df = pd.concat(pieces, ignore_index=True)
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["fail_count"] = pd.to_numeric(df["fail_count"], errors="coerce").fillna(0.0)
    out = df.groupby("stock_code", as_index=False)["fail_count"].sum()
    out["failure_penalty_score"] = (100.0 - out["fail_count"] * 12.0).clip(35, 100)
    return out


def _load_sector_industry_memberships(
    engine: Engine,
    trade_date: str,
    *,
    decision_known_at: datetime | str | None = None,
) -> pd.DataFrame:
    """Load one immutable effective-date QMT industry membership proof."""
    target = date.fromisoformat(str(trade_date))
    cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(
            target + timedelta(days=1),
            datetime.min.time(),
        )
    )
    if (
        _table_exists(engine, "qmt_membership_snapshot_run")
        and _table_exists(engine, "qmt_industry_member_snapshot")
    ):
        try:
            from server.engine.strategy_industry_history import (
                prepare_industry_history,
                resolve_analysis_industry_membership_binding,
            )

            binding = resolve_analysis_industry_membership_binding(
                engine,
                trade_date=target.isoformat(),
                decision_known_at=cutoff,
            )
            source = str(binding["source"])
            if binding.get("proof_mode") == "EXACT_QMT_MEMBERSHIP_SNAPSHOT":
                rows = pd.read_sql(
                    text(
                        """
                        SELECT stock_code, industry_name
                        FROM qmt_industry_member_snapshot
                        WHERE snapshot_date = :snapshot_date
                          AND source = :source
                          AND quality_status = 'QMT_VALIDATED'
                          AND captured_at = :captured_at
                          AND industry_type IN
                              ('L1', '一级行业', '申万一级', 'SW2021')
                        """
                    ),
                    engine,
                    params={
                        "snapshot_date": target,
                        "source": source,
                        "captured_at": binding["captured_at"],
                    },
                )
            else:
                report, history_rows = prepare_industry_history(
                    engine,
                    trade_date=target.isoformat(),
                    source=source,
                )
                if report.get("snapshot_id") != binding["proof_sha256"]:
                    raise RuntimeError(
                        "effective industry proof changed during analysis read"
                    )
                rows = pd.DataFrame.from_records(
                    [
                        {
                            "stock_code": item["stock_code"],
                            "industry_name": item["industry_name"],
                        }
                        for item in history_rows
                    ]
                )
            if not rows.empty:
                rows = rows.drop_duplicates("stock_code", keep="first")
                rows["industry_pit_status"] = PIT_AVAILABLE
                rows["industry_pit_reason"] = (
                    "PIT_PREVIOUS_OPEN_SESSION_QMT_CARRY_FORWARD"
                    if binding["previous_session_fallback"]
                    else "PIT_EXACT_DATE_QMT_SNAPSHOT"
                )
                rows["industry_snapshot_date"] = target.isoformat()
                rows["industry_snapshot_source"] = source
                rows["membership_proof_sha256"] = binding["proof_sha256"]
                rows["industry_source_snapshot_date"] = binding[
                    "source_snapshot_date"
                ]
                rows["industry_previous_session_fallback"] = bool(
                    binding["previous_session_fallback"]
                )
                rows["industry_fallback_reason"] = binding[
                    "fallback_reason"
                ]
                return rows
        except Exception as exc:
            logger.warning("Immutable effective-date industry snapshot blocked: %s", exc)
    return pd.DataFrame({"stock_code": []})


def load_sector_rotation_features(
    engine: Engine,
    trade_date: str,
    *,
    decision_known_at: datetime | str | None = None,
) -> pd.DataFrame:
    target = date.fromisoformat(trade_date)
    decision_cutoff = normalize_decision_at(
        decision_known_at
        or datetime.combine(target, datetime.max.time()).replace(microsecond=0)
    )
    memberships = _load_sector_industry_memberships(
        engine,
        trade_date,
        decision_known_at=decision_cutoff,
    )
    if memberships.empty:
        return pd.DataFrame({"stock_code": []})

    def membership_projection(
        sector_scores: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        codes = memberships.copy()
        for column, default in {
            "industry_pit_status": PIT_AVAILABLE,
            "industry_pit_reason": "PIT_EXACT_DATE_QMT_SNAPSHOT",
            "membership_proof_sha256": "",
            "industry_source_snapshot_date": trade_date,
            "industry_previous_session_fallback": False,
            "industry_fallback_reason": "",
        }.items():
            if column not in codes.columns:
                codes[column] = default
        codes["stock_code"] = (
            codes["stock_code"].astype(str).str.strip().str.zfill(6)
        )
        if sector_scores is None or sector_scores.empty:
            codes["sector_rotation_score"] = 55.0
            out = codes
        else:
            out = codes.merge(
                sector_scores[["industry_name", "sector_rotation_score"]],
                on="industry_name",
                how="left",
            )
            out["sector_rotation_score"] = pd.to_numeric(
                out["sector_rotation_score"], errors="coerce"
            ).fillna(55.0)
        return out[[
            "stock_code", "industry_name", "sector_rotation_score",
            "industry_pit_status", "industry_pit_reason",
            "industry_snapshot_date", "industry_snapshot_source",
            "membership_proof_sha256", "industry_source_snapshot_date",
            "industry_previous_session_fallback", "industry_fallback_reason",
        ]]

    dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        3,
        known_at_column="received_at",
        decision_known_at=decision_cutoff,
    )
    if not dates:
        return membership_projection()
    start_date = dates[-1]
    flow_join = ""
    main_flow_select = "0 AS main_net_inflow"
    flow_etl_select = "NULL AS flow_etl_sync_at"
    if _table_exists(engine, "sm_stock_capital_flow_daily"):
        flow_join = """
            LEFT JOIN sm_stock_capital_flow_daily f
              ON f.stock_code = k.stock_code AND f.trade_date = k.trade_date
             AND f.etl_sync_at IS NOT NULL
             AND f.etl_sync_at >= TIMESTAMP(f.trade_date, '15:10:00')
             AND f.etl_sync_at <= :decision_known_at
        """
        main_flow_select = "COALESCE(f.main_net_inflow, 0) AS main_net_inflow"
        flow_etl_select = "f.etl_sync_at AS flow_etl_sync_at"
    sql = f"""
        SELECT k.stock_code, k.trade_date, k.change_pct, k.amount,
               {main_flow_select}, {flow_etl_select}
        FROM sm_stock_kline k
        {flow_join}
        WHERE k.k_type = 1
          AND k.adjust_type = 0
          AND k.trade_date >= :start_date
          AND k.trade_date <= :trade_date
          AND k.received_at IS NOT NULL
          AND k.received_at <= :decision_known_at
    """
    observations = pd.read_sql(
        text(sql), engine,
        params={
            "start_date": start_date,
            "trade_date": trade_date,
            "decision_known_at": decision_cutoff,
        },
    )
    if observations.empty:
        return membership_projection()
    if (
        "flow_etl_sync_at" not in observations.columns
        or observations["flow_etl_sync_at"].isna().any()
    ):
        logger.warning(
            "DATA_BLOCKED: sector rotation lacks complete post-close flow: "
            "trade_date=%s",
            trade_date,
        )
        return membership_projection()
    observations["stock_code"] = (
        observations["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    observations = observations.merge(
        memberships[["stock_code", "industry_name"]],
        on="stock_code",
        how="inner",
    )
    observations["change_pct"] = pd.to_numeric(
        observations["change_pct"], errors="coerce"
    ).fillna(0.0)
    observations["amount"] = pd.to_numeric(
        observations["amount"], errors="coerce"
    ).fillna(0.0)
    observations["main_net_inflow"] = pd.to_numeric(
        observations["main_net_inflow"], errors="coerce"
    ).fillna(0.0)
    sector = observations.groupby("industry_name", as_index=False).agg(
        avg_change_3d=("change_pct", "mean"),
        total_amount=("amount", "sum"),
        total_main_net_inflow=("main_net_inflow", "sum"),
    )
    sector["flow_ratio_3d"] = (
        sector["total_main_net_inflow"]
        / sector["total_amount"].replace(0, np.nan)
        * 100.0
    ).fillna(0.0)
    if sector.empty:
        return membership_projection()
    sector["avg_change_3d"] = pd.to_numeric(sector["avg_change_3d"], errors="coerce").fillna(0.0)
    sector["flow_ratio_3d"] = pd.to_numeric(sector["flow_ratio_3d"], errors="coerce").fillna(0.0)
    base = 55.0 + _series_score(sector["flow_ratio_3d"], -0.8, 1.8) * 0.30
    overheated = pd.Series(np.where(sector["avg_change_3d"] >= 5.0, 12.0, 0.0), index=sector.index)
    early_rotation = pd.Series(
        np.where((sector["flow_ratio_3d"] > 0.25) & (sector["avg_change_3d"].between(-1.5, 2.5)), 12.0, 0.0),
        index=sector.index,
    )
    sector["sector_rotation_score"] = (base + early_rotation - overheated).clip(30, 100)
    return membership_projection(sector)


def _complete_membership_proof_scope(
    sector: pd.DataFrame,
    stock_codes: Iterable[str],
) -> pd.DataFrame:
    """Carry one whole-snapshot proof to codes with no L1 membership row."""

    codes = sorted({
        str(code or "").strip().zfill(6)
        for code in stock_codes
        if str(code or "").strip()
    })
    if sector.empty or not codes or "stock_code" not in sector.columns:
        return sector
    out = sector.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)
    missing = sorted(set(codes) - set(out["stock_code"]))
    if not missing:
        return out
    proof_columns = (
        "industry_snapshot_date",
        "industry_snapshot_source",
        "membership_proof_sha256",
        "industry_source_snapshot_date",
        "industry_previous_session_fallback",
        "industry_fallback_reason",
    )
    if not set(proof_columns).issubset(out.columns):
        return out
    proofs = out[list(proof_columns)].drop_duplicates()
    if len(proofs) != 1:
        raise RuntimeError(
            "DATA_BLOCKED: industry membership proof is ambiguous across scope"
        )
    proof = proofs.iloc[0].to_dict()
    proof_hash = str(proof.get("membership_proof_sha256") or "").lower()
    if re.fullmatch(r"[0-9a-f]{64}", proof_hash) is None:
        return out
    additions = pd.DataFrame([
        {
            "stock_code": code,
            "industry_name": "",
            "sector_rotation_score": 55.0,
            "industry_pit_status": PIT_DATA_BLOCKED,
            "industry_pit_reason": "PIT_INDUSTRY_L1_MEMBERSHIP_ABSENT",
            **proof,
        }
        for code in missing
    ])
    return pd.concat([out, additions], ignore_index=True)


def _strategy_score(row: dict[str, Any], strategy: str) -> float:
    if strategy == "ultra_short":
        return _safe_number(row.get("ultra_short_score"), 0.0)
    if strategy == "swing":
        return _safe_number(row.get("swing_score"), 0.0)
    if strategy == "main_wave":
        return _safe_number(row.get("main_wave_score"), 0.0)
    return _safe_number(row.get("short_term_score"), 0.0)


def select_primary_strategy(row: dict[str, Any]) -> str:
    if str(row.get("main_wave_signal") or "").upper() in {"REDUCE", "SELL_ALERT"}:
        return "main_wave"
    scores = {name: _strategy_score(row, name) for name in STRATEGY_PROFILES}
    qualified = {
        name: score
        for name, score in scores.items()
        if score >= STRATEGY_PROFILES[name]["min_score"]
    }
    pool = qualified or scores
    return max(pool, key=lambda name: (pool[name], STRATEGY_PROFILES[name]["confirm_score"] * -1))


def _position_weight(row: dict[str, Any], strategy: str, status: str) -> float:
    profile = STRATEGY_PROFILES[strategy]
    score = _safe_number(row.get("final_trade_score"), _strategy_score(row, strategy))
    weight = float(profile["base_position"]) + max(0.0, score - float(profile["min_score"])) * 0.16
    mood = _safe_number(row.get("market_mood_score"), 50.0)
    risk_level = str(row.get("event_risk_level") or "LOW").upper()
    if status not in {"CONFIRM", "BUY_READY"}:
        weight *= 0.55
    if mood < 35:
        weight *= 0.60
    elif mood > 65:
        weight *= 1.12
    if risk_level == "MEDIUM":
        weight *= 0.75
    return round(max(1.0, min(12.0, weight)), 2)


def _parse_date_value(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def build_strategy_trade_plan(row: dict[str, Any], strategy: str) -> dict[str, Any]:
    close = _first_price(row.get("close"))
    ma5 = _first_price(row.get("ma5"), default=close)
    ma10 = _first_price(row.get("ma10"), default=ma5 or close)
    ma20 = _first_price(row.get("ma20"), default=ma10 or close)
    ma60 = _first_price(row.get("ma60"), default=ma20 or close)
    profile = STRATEGY_PROFILES[strategy]
    score = _strategy_score(row, strategy)
    amount = _safe_number(row.get("amount"), 0.0)
    change_pct = _safe_number(row.get("change_pct"), 0.0)
    mood = _safe_number(row.get("market_mood_score"), 50.0)
    base_status = str(row.get("recommend_status") or "SUSPENDED").upper()
    event_risk = str(row.get("event_risk_level") or "LOW").upper()
    quality_score = _safe_number(row.get("quality_score"), _safe_number(row.get("ai_score"), score))
    entry_score = _safe_number(row.get("entry_score"), score)
    final_trade_score = _safe_number(row.get("final_trade_score"), score)
    expected_return_pct = _safe_number(row.get("expected_return_pct"), 0.0)
    heat_overload_score = _safe_number(row.get("heat_overload_score"), 60.0)
    confidence_score = _safe_number(row.get("confidence_score"), 62.0)
    failure_penalty_score = _safe_number(row.get("failure_penalty_score"), 100.0)
    main_wave_score = _safe_number(row.get("main_wave_score"), 0.0)
    trend_hold_score = _safe_number(row.get("trend_hold_score"), 0.0)
    main_wave_signal = str(row.get("main_wave_signal") or "NONE").upper()
    main_wave_reason = str(row.get("main_wave_reason") or "")
    max_recent_trade_score = _safe_number(row.get("max_recent_trade_score"), 0.0)
    last_pick_date = _parse_date_value(row.get("last_pick_date"))
    current_trade_date = _parse_date_value(row.get("trade_date"))
    cooldown_days = int(profile.get("cooldown_days", 0) or 0)
    cooldown_days_left = 0
    cooldown_until = None
    if last_pick_date and current_trade_date and cooldown_days > 0:
        days_since = max(0, (current_trade_date - last_pick_date).days)
        cooldown_days_left = max(0, cooldown_days - days_since)
        if cooldown_days_left > 0:
            cooldown_until = last_pick_date + timedelta(days=cooldown_days)
    cooldown_bypassed = cooldown_days_left > 0 and final_trade_score >= max_recent_trade_score + 3.0

    if close <= 0:
        return {
            "primary_strategy": strategy,
            "strategy_profile": strategy,
            "signal_status": "WATCH",
            "signal_reason": "missing close price",
            "entry_price_low": None,
            "entry_price_high": None,
            "stop_loss_price": None,
            "take_profit_1": None,
            "take_profit_2": None,
            "position_weight": None,
            "max_holding_days": int(profile["max_holding_days"]),
            "entry_conditions_json": "[]",
            "sell_rules_json": "[]",
            "invalidation_reason": "price data unavailable",
            "cooldown_days_left": 0,
            "cooldown_until": None,
        }

    if strategy == "ultra_short":
        entry_low = max(close * 0.985, ma5 * 0.995)
        entry_high = min(close * 1.018, close * 1.095)
        entry_conditions = [
            "live quote is fresh",
            "price stays near VWAP/MA5 or breaks intraday high",
            "main capital flow remains positive",
            "do not chase limit-up or extended gap",
        ]
        invalidation = "breaks MA5/VWAP, capital flow turns negative, or intraday rise is overextended"
    elif strategy == "main_wave":
        anchor = _first_price(ma10, ma5, close, default=close)
        entry_low = max(anchor * 0.985, close * 0.94)
        entry_high = min(close * 1.025, close * 1.095)
        entry_conditions = [
            "main wave score confirms breakout or trend continuation",
            "prefer pullback holding MA5/MA10 instead of chasing a limit-up candle",
            "volume remains above the 20-day average but is not a blow-off spike",
            "sector rotation score stays strong and trend hold score does not deteriorate",
        ]
        invalidation = "closes below MA20, high-volume long bearish candle appears, or main-wave signal turns SELL_ALERT"
    elif strategy == "swing":
        anchor = _first_price(ma20, ma60, close, default=close)
        entry_low = anchor * 0.98
        entry_high = min(anchor * 1.025, close * 1.095)
        entry_conditions = [
            "price holds MA20/MA60 support",
            "medium trend remains upward",
            "event risk stays LOW",
            "market breadth is not sharply weakening",
        ]
        invalidation = "closes below MA60, event risk rises, or medium trend breaks"
    else:
        anchor = _first_price(ma10, ma5, close, default=close)
        entry_low = min(close * 0.995, anchor * 0.99)
        entry_high = min(max(close * 1.012, anchor * 1.025), close * 1.095)
        entry_conditions = [
            "pullback holds MA5/MA10 or breaks 20-day platform",
            "volume remains above recent average",
            "capital flow does not reverse",
            "avoid chasing after sharp daily jump",
        ]
        invalidation = "falls below MA10/MA20, volume shrinks sharply, or capital flow weakens"

    if entry_low > entry_high:
        entry_low, entry_high = entry_high * 0.985, entry_low * 1.005

    ref_price = (entry_low + entry_high) / 2.0
    stop_loss = ref_price * (1.0 + float(profile["stop_loss_pct"]) / 100.0)
    if strategy == "main_wave":
        trend_stop = _first_price(row.get("trend_stop_price"), ma20 * 0.97, default=stop_loss)
        stop_loss = min(stop_loss, trend_stop) if trend_stop > 0 else stop_loss
    take_profit_1 = ref_price * (1.0 + float(profile["take_profit_1_pct"]) / 100.0)
    take_profit_2 = ref_price * (1.0 + float(profile["take_profit_2_pct"]) / 100.0)

    blockers: list[str] = []
    if strategy == "main_wave" and main_wave_signal in {"SELL_ALERT", "REDUCE"}:
        status = "SELL_ALERT"
        blockers.append(main_wave_reason or "main-wave risk signal triggered")
    elif base_status == "BLOCK" or event_risk == "CRITICAL":
        status = "BLOCK"
        blockers.append("blocked by base recommendation gate")
    elif strategy != "main_wave" and expected_return_pct < 5.0:
        status = "BLOCK"
        blockers.append(f"expected upside {expected_return_pct:.1f}% is below 5% threshold")
    elif base_status != "ALLOW":
        if strategy == "main_wave" and main_wave_score >= float(profile["min_score"]) and event_risk == "LOW":
            status = "WATCH"
            blockers.append(f"base status is {base_status}; main-wave candidate should wait for tradable pullback")
        else:
            status = "WATCH"
            blockers.append(f"base status is {base_status}")
    elif entry_score < 45.0:
        status = "WATCH"
        blockers.append(f"entry score {entry_score:.1f} is weak; good stock but poor buy point")
    elif cooldown_days_left > 0 and not cooldown_bypassed:
        status = "WATCH"
        blockers.append(f"cooldown active for {cooldown_days_left} more days")
    elif change_pct >= 9.7:
        status = "WATCH"
        blockers.append("near daily limit-up; wait for tradable pullback")
    elif amount < 50_000_000:
        status = "WATCH"
        blockers.append("liquidity is below intraday trading threshold")
    elif mood < 30 and strategy != "swing":
        status = "WATCH"
        blockers.append("market mood is weak")
    elif heat_overload_score < 50.0:
        status = "WATCH"
        blockers.append("heat overload is high; avoid becoming exit liquidity")
    elif confidence_score < 45.0:
        status = "WATCH"
        blockers.append("recent recommendation score is unstable")
    elif failure_penalty_score < 55.0:
        status = "WATCH"
        blockers.append("recent failure samples require caution")
    elif (
        strategy == "main_wave"
        and main_wave_signal == "BUY_READY"
        and main_wave_score >= float(profile["confirm_score"])
        and trend_hold_score >= 58.0
    ):
        status = "BUY_READY"
    elif final_trade_score >= float(profile["confirm_score"]) and entry_score >= 55.0:
        status = "CONFIRM"
    else:
        status = "WATCH"
        blockers.append("final trade score or entry score has not reached confirm threshold")

    if status == "BUY_READY":
        reason = (
            f"main-wave buy ready: score {main_wave_score:.1f}, hold {trend_hold_score:.1f}; "
            f"{main_wave_reason or 'wait for pullback/volume confirmation'}"
        )
    elif status == "CONFIRM":
        reason = (
            f"{strategy} final {final_trade_score:.1f} confirms candidate "
            f"(quality {quality_score:.1f}, entry {entry_score:.1f}); wait for intraday trigger"
        )
    else:
        reason = "; ".join(blockers) if blockers else f"{strategy} candidate needs confirmation"

    sell_rules = [
        f"stop loss below {abs(float(profile['stop_loss_pct'])):.1f}%",
        f"take profit levels {float(profile['take_profit_1_pct']):.1f}%/{float(profile['take_profit_2_pct']):.1f}%",
        f"max holding {int(profile['max_holding_days'])} trading days",
        invalidation,
    ]
    if strategy == "main_wave":
        sell_rules = [
            "do not exit only because fixed profit target is reached",
            "reduce when distance from MA20 is excessive and cumulative wave gain is high",
            "sell alert when price closes below MA20 after a main-wave advance",
            f"trend stop reference { _round_price(row.get('trend_stop_price')) or 'MA20 trailing stop' }",
        ]

    return {
        "primary_strategy": strategy,
        "strategy_profile": strategy,
        "signal_status": status,
        "signal_reason": reason[:500],
        "entry_price_low": _round_price(entry_low),
        "entry_price_high": _round_price(entry_high),
        "stop_loss_price": _round_price(stop_loss),
        "take_profit_1": _round_price(take_profit_1),
        "take_profit_2": _round_price(take_profit_2),
        "position_weight": _position_weight(row, strategy, status),
        "max_holding_days": int(profile["max_holding_days"]),
        "entry_conditions_json": json.dumps(entry_conditions, ensure_ascii=False),
        "sell_rules_json": json.dumps(sell_rules, ensure_ascii=False),
        "invalidation_reason": invalidation[:500],
        "cooldown_days_left": cooldown_days_left if not cooldown_bypassed else 0,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until and not cooldown_bypassed else None,
    }


def add_strategy_signals(
    df: pd.DataFrame,
    confidence: pd.DataFrame | None = None,
    rec_history: pd.DataFrame | None = None,
    failures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = df.copy()
    for extra in (confidence, rec_history, failures):
        if extra is not None and not extra.empty and "stock_code" in extra.columns:
            extra = extra.copy()
            extra["stock_code"] = extra["stock_code"].astype(str).str.strip().str.zfill(6)
            out = out.merge(extra, on="stock_code", how="left")

    amount = _numeric_col(out, "amount")
    turnover = _numeric_col(out, "turnover_ratio")
    change_pct = _numeric_col(out, "change_pct")
    dist_ma20 = _numeric_col(out, "dist_ma20")
    volatility = _numeric_col(out, "volatility_20")
    close = _numeric_col(out, "close")
    amount_ratio = _numeric_col(out, "amount_ratio_5")

    liquidity_score = _round_score(
        _series_score(amount, 30_000_000, 600_000_000) * 0.70
        + _series_score(turnover, 0.5, 8.0) * 0.30
    )
    ultra_volatility_fit = (100 - (volatility - 5.0).abs() * 10).clip(35, 100).fillna(55)
    ultra_penalty = (
        pd.Series(np.where(change_pct >= 7.0, 6.0, 0.0), index=out.index)
        + pd.Series(np.where(dist_ma20 >= 14.0, 6.0, 0.0), index=out.index)
        + pd.Series(np.where(amount < 50_000_000, 8.0, 0.0), index=out.index)
    )
    out["ultra_short_score"] = _round_score(
        out["technical_score"] * 0.30
        + out["capital_score"] * 0.28
        + out["sentiment_score"] * 0.20
        + out["event_score"] * 0.10
        + liquidity_score * 0.08
        + ultra_volatility_fit * 0.04
        - ultra_penalty
    )

    swing_trend = (
        (pd.to_numeric(out.get("close"), errors="coerce") > pd.to_numeric(out.get("ma20"), errors="coerce")).astype(float) * 55
        + (pd.to_numeric(out.get("ma20"), errors="coerce") > pd.to_numeric(out.get("ma60"), errors="coerce")).astype(float) * 45
    )
    swing_penalty = (
        pd.Series(np.where(change_pct >= 8.0, 5.0, 0.0), index=out.index)
        + pd.Series(np.where(pd.to_numeric(out.get("drawdown_60"), errors="coerce") <= -25.0, 6.0, 0.0), index=out.index)
    )
    out["swing_score"] = _round_score(
        out["long_term_score"] * 0.42
        + out["technical_score"] * 0.18
        + out["capital_score"] * 0.10
        + out["event_score"] * 0.10
        + out["risk_score"] * 0.12
        + swing_trend * 0.08
        - swing_penalty
    )

    out["quality_score"] = _round_score(out["ai_score"])

    resistance = pd.concat([
        _numeric_col(out, "high_20"),
        _numeric_col(out, "high_60"),
    ], axis=1).max(axis=1)
    resistance = resistance.where(resistance > 0, np.nan)
    out["resistance_price"] = resistance.round(2)
    expected_return_pct = (resistance / close.replace(0, np.nan) - 1.0) * 100.0
    out["expected_return_pct"] = pd.to_numeric(expected_return_pct, errors="coerce").fillna(0.0).round(2)
    out["expected_return_score"] = _round_score(_series_score(out["expected_return_pct"], 5.0, 20.0, default=45.0))

    fused_rank = _numeric_col(out, "fused_rank")
    heat_score = pd.Series(60.0, index=out.index)
    heat_score = heat_score.mask((fused_rank > 0) & (fused_rank <= 5), 45.0)
    heat_score = heat_score.mask((fused_rank > 5) & (fused_rank <= 10), 55.0)
    heat_score = heat_score.mask((fused_rank > 10) & (fused_rank <= 30), 85.0)
    heat_score = heat_score.mask((fused_rank > 30) & (fused_rank <= 60), 75.0)
    heat_score = heat_score.mask((fused_rank > 60) & (fused_rank <= 100), 68.0)
    out["heat_overload_score"] = _round_score(heat_score)

    out["confidence_score"] = _numeric_col(out, "confidence_score", 62.0).fillna(62.0).clip(35, 100).round(1)
    out["failure_penalty_score"] = _numeric_col(out, "failure_penalty_score", 100.0).fillna(100.0).clip(35, 100).round(1)
    out["sector_rotation_score"] = _numeric_col(out, "sector_rotation_score", 55.0).fillna(55.0).clip(30, 100).round(1)

    chase_score = (
        100
        - _series_score(change_pct, 3.0, 8.0, default=45.0) * 0.45
        - _series_score(dist_ma20, 4.0, 15.0, default=45.0) * 0.35
        - _series_score(amount_ratio, 1.8, 3.5, default=20.0) * 0.20
    ).clip(0, 100)
    liquidity_fit = liquidity_score
    out["entry_score"] = _round_score(
        chase_score * 0.28
        + out["expected_return_score"] * 0.26
        + out["heat_overload_score"] * 0.16
        + liquidity_fit * 0.12
        + out["sector_rotation_score"] * 0.10
        + out["confidence_score"] * 0.08
    )
    out["final_trade_score"] = _round_score(out["quality_score"] * 0.70 + out["entry_score"] * 0.30)
    out["max_recent_trade_score"] = _numeric_col(out, "max_recent_trade_score", 0.0).fillna(0.0)

    ma5 = _numeric_col(out, "ma5")
    ma10 = _numeric_col(out, "ma10")
    ma20 = _numeric_col(out, "ma20")
    ma60 = _numeric_col(out, "ma60")
    high_20 = _numeric_col(out, "high_20")
    high_60 = _numeric_col(out, "high_60")
    from_low_60 = _numeric_col(out, "from_low_60", 0.0).fillna(0.0)
    amount_ratio_20 = _numeric_col(out, "amount_ratio_20", 1.0).fillna(1.0).clip(0, 5)
    dist_ma20_main = _numeric_col(out, "dist_ma20", 0.0).fillna(0.0)

    trend_alignment_score = (
        (close > ma5).astype(float) * 18.0
        + (ma5 > ma10).astype(float) * 18.0
        + (ma10 > ma20).astype(float) * 22.0
        + (ma20 >= ma60 * 0.96).astype(float) * 17.0
        + (close > ma20).astype(float) * 25.0
    )
    near_high20_score = _series_score((close / high_20.replace(0, np.nan)) * 100.0, 94.0, 101.0, default=45.0)
    near_high60_score = _series_score((close / high_60.replace(0, np.nan)) * 100.0, 88.0, 100.0, default=45.0)
    breakout_score = pd.concat([near_high20_score, near_high60_score], axis=1).max(axis=1)
    volume_wave_score = (100.0 - (amount_ratio_20 - 1.6).abs() * 35.0).clip(35, 100)
    strength_score = _series_score(_numeric_col(out, "pct_20", 0.0), 5.0, 45.0, default=45.0)
    over_extension_penalty = (
        pd.Series(np.where((dist_ma20_main >= 35.0) & (from_low_60 >= 120.0), 10.0, 0.0), index=out.index)
        + pd.Series(np.where((change_pct >= 9.7) & (amount_ratio_20 >= 2.5), 6.0, 0.0), index=out.index)
    )
    out["main_wave_score"] = _round_score(
        trend_alignment_score * 0.32
        + breakout_score * 0.18
        + volume_wave_score * 0.14
        + out["capital_score"] * 0.12
        + out["sector_rotation_score"] * 0.12
        + strength_score * 0.08
        + out["quality_score"] * 0.04
        - over_extension_penalty
    )
    hold_penalty = (
        pd.Series(np.where(close < ma10, 24.0, 0.0), index=out.index)
        + pd.Series(np.where(close < ma20, 46.0, 0.0), index=out.index)
        + pd.Series(np.where((dist_ma20_main >= 28.0) & (from_low_60 >= 120.0), 18.0, 0.0), index=out.index)
        + pd.Series(np.where((change_pct <= -6.0) & (amount_ratio_20 >= 1.15), 18.0, 0.0), index=out.index)
    )
    hold_bonus = (
        (close > ma10).astype(float) * 10.0
        + (close > ma20).astype(float) * 10.0
        + (ma10 > ma20).astype(float) * 8.0
        + (ma20 >= ma60 * 0.96).astype(float) * 6.0
    )
    out["trend_hold_score"] = _round_score(64.0 + hold_bonus - hold_penalty)
    out["trend_stop_price"] = (ma20 * 0.97).round(2)
    out["trend_reduce_price"] = (ma10 * 0.98).round(2)

    stages: list[str] = []
    signals: list[str] = []
    reasons: list[str] = []
    for row in out.to_dict(orient="records"):
        mw = _safe_number(row.get("main_wave_score"), 0.0)
        hold = _safe_number(row.get("trend_hold_score"), 0.0)
        d20 = _safe_number(row.get("dist_ma20"), 0.0)
        low60_gain = _safe_number(row.get("from_low_60"), 0.0)
        chg = _safe_number(row.get("change_pct"), 0.0)
        ar20 = _safe_number(row.get("amount_ratio_20"), 1.0)
        c = _safe_number(row.get("close"), 0.0)
        r_ma10 = _safe_number(row.get("ma10"), 0.0)
        r_ma20 = _safe_number(row.get("ma20"), 0.0)

        if mw >= 78 and d20 >= 28 and low60_gain >= 120:
            stage = "EXTENDED"
        elif mw >= 74:
            stage = "BREAKOUT"
        elif mw >= 70:
            stage = "TRENDING"
        elif mw >= 62:
            stage = "WATCH_BASE"
        else:
            stage = "NONE"

        if c > 0 and r_ma20 > 0 and c < r_ma20 and low60_gain >= 120:
            signal = "SELL_ALERT"
            reason = "主升浪跌破MA20，趋势破坏，优先清仓或大幅减仓"
        elif c > 0 and r_ma10 > 0 and c < r_ma10 and low60_gain >= 150:
            signal = "REDUCE"
            reason = "高位跌破MA10，主升斜率转弱，先减仓并观察MA20"
        elif d20 >= 28 and low60_gain >= 120:
            signal = "REDUCE"
            reason = "距离MA20过大且累计涨幅高，进入高位扩张区，提示减仓/收紧止盈"
        elif chg <= -6.0 and ar20 >= 1.15 and low60_gain >= 80:
            signal = "REDUCE"
            reason = "高位放量长阴，主升浪风险升高"
        elif mw >= 74 and hold >= 58 and chg < 9.7:
            signal = "BUY_READY"
            reason = "主升浪放量突破且趋势保持，等待回踩MA5/MA10或盘中确认"
        elif mw >= 70 and hold >= 50:
            signal = "WATCH"
            reason = "主升浪雏形成立，等待突破确认或缩量回踩"
        else:
            signal = "NONE"
            reason = "尚未形成主升浪买卖点"

        stages.append(stage)
        signals.append(signal)
        reasons.append(reason)

    out["main_wave_stage"] = stages
    out["main_wave_signal"] = signals
    out["main_wave_reason"] = reasons

    plans = []
    suitable_col: list[str] = []
    for row in out.to_dict(orient="records"):
        primary = select_primary_strategy(row)
        plan = build_strategy_trade_plan(row, primary)
        suitable = [
            name for name in STRATEGY_PROFILES
            if _strategy_score(row, name) >= float(STRATEGY_PROFILES[name]["min_score"])
        ]
        if not suitable and plan["signal_status"] != "BLOCK":
            suitable = [primary]
        plans.append(plan)
        suitable_col.append(json.dumps(suitable, ensure_ascii=False))

    for key in [
        "primary_strategy", "strategy_profile", "signal_status", "signal_reason",
        "entry_price_low", "entry_price_high", "stop_loss_price",
        "take_profit_1", "take_profit_2", "position_weight",
        "max_holding_days", "entry_conditions_json", "sell_rules_json",
        "invalidation_reason", "cooldown_days_left", "cooldown_until",
    ]:
        out[key] = [plan.get(key) for plan in plans]
    out["suitable_strategies"] = suitable_col
    out["model_version"] = MODEL_VERSION
    return out


def compute_scores(
    kline: pd.DataFrame,
    finance: pd.DataFrame,
    flow: pd.DataFrame,
    hot: pd.DataFrame,
    notices: pd.DataFrame,
    market_mood_score: float,
    flow_date: str,
    trade_date: str,
    min_score: float,
    sector: pd.DataFrame | None = None,
    confidence: pd.DataFrame | None = None,
    rec_history: pd.DataFrame | None = None,
    failures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    flow = _ensure_columns(flow, {
        "main_net_inflow": np.nan,
        "main_net_inflow_5d": np.nan,
        "main_net_inflow_20d": np.nan,
        "flow_trade_date": None,
    })
    hot = _ensure_columns(hot, {
        "fused_rank": np.nan,
        "hot_total_score": np.nan,
        "source_flag": "",
        "industry_name": "",
    })
    notices = _ensure_columns(notices, {
        "notice_count": 0,
        "notice_positive": 0,
        "notice_negative": 0,
        "notice_critical": 0,
        "latest_notice_date": None,
        "latest_notice_time": None,
        "risk_titles": None,
        "positive_titles": None,
        "event_revision_ids": None,
        "event_content_hashes": None,
        "event_pit_status": PIT_DATA_BLOCKED,
        "event_pit_reason": "PIT_EVENT_COVERAGE_UNPROVEN",
        "event_manifest_hash": "",
        "event_authoritative_empty": False,
        "event_coverage_id": None,
        "event_coverage_response_hash": None,
        "event_coverage_watermark_hash": None,
        "event_covered_through_at": None,
    })
    sector = _ensure_columns(sector if sector is not None else pd.DataFrame({"stock_code": []}), {
        "industry_name": "",
        "sector_rotation_score": 55.0,
        "industry_pit_status": PIT_DATA_BLOCKED,
        "industry_pit_reason": "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_REQUIRED",
        "industry_snapshot_date": "",
        "industry_snapshot_source": "",
        "membership_proof_sha256": "",
        "industry_source_snapshot_date": "",
        "industry_previous_session_fallback": False,
        "industry_fallback_reason": "",
    })
    if not sector.empty and "industry_name" in sector.columns:
        sector = sector.rename(columns={"industry_name": "sector_industry_name"})
    df = kline.merge(finance, on="stock_code", how="left")
    df = df.merge(flow, on="stock_code", how="left", suffixes=("", "_flow"))
    df = df.merge(hot[["stock_code", "fused_rank", "hot_total_score", "source_flag", "industry_name"]], on="stock_code", how="left")
    if not sector.empty:
        df = df.merge(
            sector[[
                "stock_code", "sector_industry_name",
                "sector_rotation_score", "industry_pit_status",
                "industry_pit_reason", "industry_snapshot_date",
                "industry_snapshot_source", "membership_proof_sha256",
                "industry_source_snapshot_date",
                "industry_previous_session_fallback",
                "industry_fallback_reason",
            ]],
            on="stock_code",
            how="left",
        )
        df["industry_name"] = df["industry_name"].fillna("").astype(str)
        df["sector_industry_name"] = df["sector_industry_name"].fillna("").astype(str)
        df["industry_name"] = df["industry_name"].where(df["industry_name"] != "", df["sector_industry_name"])
    df = df.merge(notices, on="stock_code", how="left")

    if "finance_pit_status" not in df.columns:
        df["finance_pit_status"] = PIT_DATA_BLOCKED
    df["finance_pit_status"] = df["finance_pit_status"].fillna(
        PIT_DATA_BLOCKED
    )
    if "finance_pit_reason" not in df.columns:
        df["finance_pit_reason"] = "PIT_FINANCE_COVERAGE_UNPROVEN"
    if "finance_manifest_hash" not in df.columns:
        df["finance_manifest_hash"] = ""
    df["event_pit_status"] = df["event_pit_status"].fillna(PIT_DATA_BLOCKED)
    df["event_pit_reason"] = df["event_pit_reason"].fillna(
        "PIT_EVENT_COVERAGE_UNPROVEN"
    )
    if "industry_pit_status" not in df.columns:
        df["industry_pit_status"] = PIT_DATA_BLOCKED
    df["industry_pit_status"] = df["industry_pit_status"].fillna(
        PIT_DATA_BLOCKED
    )
    if "industry_pit_reason" not in df.columns:
        df["industry_pit_reason"] = (
            "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_REQUIRED"
        )
    for column in (
        "industry_source_snapshot_date",
        "industry_fallback_reason",
    ):
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].fillna("")
    if "industry_previous_session_fallback" not in df.columns:
        df["industry_previous_session_fallback"] = False
    df["industry_previous_session_fallback"] = df[
        "industry_previous_session_fallback"
    ].fillna(False).astype(bool)

    for col in ["notice_count", "notice_positive", "notice_negative", "notice_critical"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)
    df["risk_titles"] = df["risk_titles"].apply(lambda x: x if isinstance(x, list) else [])
    df["positive_titles"] = df["positive_titles"].apply(lambda x: x if isinstance(x, list) else [])

    close = _numeric_col(df, "close")
    amount = _numeric_col(df, "amount")
    turnover = _numeric_col(df, "turnover_ratio")

    trend_score = (
        (close > _numeric_col(df, "ma5")).astype(float) * 12
        + (close > _numeric_col(df, "ma10")).astype(float) * 10
        + (close > _numeric_col(df, "ma20")).astype(float) * 12
        + (_numeric_col(df, "ma5") > _numeric_col(df, "ma10")).astype(float) * 8
        + (_numeric_col(df, "ma10") > _numeric_col(df, "ma20")).astype(float) * 8
    )
    technical = (
        30
        + trend_score
        + (_series_score(df["pct_5"], -8, 12) - 50) * 0.18
        + (_series_score(df["dist_ma20"], -15, 12) - 50) * 0.12
        + (_series_score(df["amount_ratio_5"], 0.5, 2.2) - 50) * 0.10
        + (_series_score(turnover, 0.5, 8.0) - 50) * 0.08
        - (_series_score(df["volatility_20"], 2.5, 9.0) - 50).clip(lower=0) * 0.10
    )
    df["technical_score"] = _round_score(technical)

    main_ratio = _numeric_col(df, "main_net_inflow") / amount.replace(0, np.nan) * 100.0
    flow5_ratio = _numeric_col(df, "main_net_inflow_5d") / (_numeric_col(df, "amount_ma5") * 5).replace(0, np.nan) * 100.0
    flow20_ratio = _numeric_col(df, "main_net_inflow_20d") / (_numeric_col(df, "amount_ma20") * 20).replace(0, np.nan) * 100.0
    capital = (
        _percentile_score(main_ratio, default=50) * 0.45
        + _percentile_score(flow5_ratio, default=50) * 0.35
        + _percentile_score(flow20_ratio, default=50) * 0.20
    )
    if flow_date and flow_date != trade_date:
        capital = capital * 0.75 + 50 * 0.25
    df["capital_score"] = _round_score(capital)

    sentiment = pd.Series(50.0, index=df.index)
    has_hot = pd.to_numeric(df["fused_rank"], errors="coerce").notna()
    sentiment.loc[has_hot] = (101 - pd.to_numeric(df.loc[has_hot, "fused_rank"], errors="coerce")).clip(0, 100) * 0.55 + 45
    sentiment = sentiment * 0.8 + float(market_mood_score) * 0.2
    df["sentiment_score"] = _round_score(sentiment)
    df["market_mood_score"] = float(market_mood_score)

    roe = _series_score(_numeric_col(df, "roe_wtd"), -5, 18)
    gross_margin = _series_score(_numeric_col(df, "gross_margin"), 5, 45)
    net_margin = _series_score(_numeric_col(df, "net_margin"), -10, 20)
    oper_cf = _series_score(_numeric_col(df, "oper_cf_ps"), -1, 2)
    df["fundamental_score"] = _round_score(roe * 0.38 + gross_margin * 0.24 + net_margin * 0.24 + oper_cf * 0.14)

    rev_growth = _series_score(_numeric_col(df, "total_rev_yoy_gr"), -20, 40)
    profit_growth = _series_score(_numeric_col(df, "net_profit_yoy_gr"), -40, 80)
    non_gaap_growth = _series_score(_numeric_col(df, "non_gaap_net_profit_yoy_gr"), -40, 80)
    df["growth_score"] = _round_score(rev_growth * 0.35 + profit_growth * 0.45 + non_gaap_growth * 0.20)

    pb = close / _numeric_col(df, "net_asset_ps").replace(0, np.nan)
    pb_score = 100 - _series_score(pb, 1.0, 9.0) * 0.70
    pb_score = pb_score.mask(pb <= 0, 35).fillna(55)
    df["valuation_score"] = _round_score(pb_score)

    debt_score = 100 - _series_score(_numeric_col(df, "asset_liab_ratio"), 30, 85) * 0.65
    cash_score = _series_score(_numeric_col(df, "cash_flow_ratio"), -0.2, 1.5)
    curr_score = _series_score(_numeric_col(df, "curr_ratio"), 0.8, 2.0)
    df["risk_score"] = _round_score(debt_score * 0.55 + cash_score * 0.25 + curr_score * 0.20)

    event_risk_score = 100 - df["notice_critical"] * 35 - df["notice_negative"] * 14 + df["notice_positive"] * 4
    df["event_risk_score"] = _round_score(event_risk_score)
    df["event_risk_level"] = np.select(
        [
            df["notice_critical"] > 0,
            df["notice_negative"] >= 2,
            df["notice_negative"] == 1,
        ],
        ["CRITICAL", "HIGH", "MEDIUM"],
        default="LOW",
    )
    event_score = 58 + df["notice_positive"] * 8 - df["notice_negative"] * 10 - df["notice_critical"] * 25
    df["event_score"] = _round_score(event_score)

    quality_scores: list[float] = []
    quality_flags: list[str] = []
    for row in df.to_dict(orient="records"):
        score, flags = build_data_quality(row, trade_date=trade_date, flow_date=flow_date)
        quality_scores.append(score)
        quality_flags.append(json.dumps(flags, ensure_ascii=False))
    df["data_quality_score"] = quality_scores
    df["data_quality_flags"] = quality_flags

    df["long_term_score"] = _round_score(
        df["fundamental_score"] * 0.34
        + df["growth_score"] * 0.28
        + df["valuation_score"] * 0.20
        + df["risk_score"] * 0.18
    )
    df["short_term_score"] = _round_score(
        df["technical_score"] * 0.34
        + df["capital_score"] * 0.28
        + df["sentiment_score"] * 0.24
        + df["event_score"] * 0.14
    )
    df["ai_score"] = _round_score(df["short_term_score"] * 0.58 + df["long_term_score"] * 0.42)

    statuses: list[str] = []
    reasons: list[str] = []
    for row in df.to_dict(orient="records"):
        status, reason = choose_recommend_status(
            row.get("stock_code", ""),
            row.get("short_name", ""),
            row.get("ai_score", 0),
            row.get("short_term_score", 0),
            row.get("long_term_score", 0),
            row.get("event_risk_level", "LOW"),
            row.get("amount"),
            row.get("change_pct"),
            min_score,
            row.get("data_quality_score", 100.0),
            json.loads(row.get("data_quality_flags") or "[]"),
        )
        statuses.append(status)
        reasons.append(reason)
    df["recommend_status"] = statuses
    df["recommend_reason"] = reasons
    df = add_strategy_signals(df, confidence=confidence, rec_history=rec_history, failures=failures)
    return df


def _json_list(items: list[str]) -> str:
    return json.dumps(items, ensure_ascii=False)


def _build_text_fields(df: pd.DataFrame, flow_date: str, trade_date: str) -> pd.DataFrame:
    summaries: list[str] = []
    recommendations: list[str] = []
    strengths_col: list[str] = []
    risks_col: list[str] = []
    event_detail_col: list[str] = []

    for row in df.to_dict(orient="records"):
        strengths: list[str] = []
        risks: list[str] = []
        try:
            quality_flags = set(json.loads(row.get("data_quality_flags") or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            quality_flags = set()
        if float(row.get("technical_score") or 0) >= 68:
            strengths.append("技术趋势较强")
        if float(row.get("capital_score") or 0) >= 68:
            strengths.append("资金流排名靠前")
        if float(row.get("fundamental_score") or 0) >= 65:
            strengths.append("基本面质量较好")
        if float(row.get("growth_score") or 0) >= 65:
            strengths.append("成长性评分较高")
        if float(row.get("sentiment_score") or 0) >= 68:
            strengths.append("市场热度较高")
        if row.get("positive_titles"):
            strengths.append("近期公告偏积极")

        if row.get("event_risk_level") in ("HIGH", "CRITICAL"):
            risks.append("近期公告存在风险事项")
        if row.get("risk_titles"):
            risks.extend([f"公告风险: {t}" for t in row.get("risk_titles", [])[:2]])
        row_flow_date = str(row.get("flow_trade_date") or "").strip()[:10]
        if "missing_flow" in quality_flags:
            risks.append("缺少目标交易日资金流，资金因子不可用于推荐")
        elif "stale_flow" in quality_flags:
            risks.append(
                f"资金流日期{row_flow_date or '未知'}不是目标交易日{trade_date}"
            )
        if "missing_hot_rank" in quality_flags:
            risks.append("目标交易日缺少至少双源热榜，热度因子已禁用")
        if float(row.get("amount") or 0) < 30_000_000:
            risks.append("成交额偏低")
        if float(row.get("change_pct") or 0) >= 9.7:
            risks.append("当日涨幅过高")
        if float(row.get("volatility_20") or 0) >= 8:
            risks.append("短期波动偏大")
        if not strengths:
            strengths.append("暂无突出优势，作为基础覆盖样本")

        status = row.get("recommend_status") or "SUSPENDED"
        summary = (
            f"基础评分: 综合{row.get('ai_score'):.1f}, 短线{row.get('short_term_score'):.1f}, "
            f"长线{row.get('long_term_score'):.1f}; 状态{status}。"
        )
        if status == "ALLOW":
            recommendation = "可进入候选池，盘中等待量价和资金二次确认，并设置止损。"
        elif status == "SUSPENDED":
            recommendation = "暂缓买入，等待评分或风险项改善后再复核。"
        else:
            recommendation = "不建议进入推荐池。"

        event_detail = {
            "notice_count_14d": int(row.get("notice_count") or 0),
            "positive_count": int(row.get("notice_positive") or 0),
            "negative_count": int(row.get("notice_negative") or 0),
            "critical_count": int(row.get("notice_critical") or 0),
            "risk_titles": row.get("risk_titles") or [],
            "positive_titles": row.get("positive_titles") or [],
            "event_pit_status": row.get("event_pit_status"),
            "event_pit_reason": row.get("event_pit_reason"),
            "event_manifest_hash": row.get("event_manifest_hash"),
            "event_revision_ids": row.get("event_revision_ids") or [],
            "event_content_hashes": row.get("event_content_hashes") or [],
            "event_authoritative_empty": bool(
                row.get("event_authoritative_empty")
            ),
            "event_coverage_id": row.get("event_coverage_id"),
            "event_coverage_response_hash": row.get(
                "event_coverage_response_hash"
            ),
            "event_coverage_watermark_hash": row.get(
                "event_coverage_watermark_hash"
            ),
            "event_covered_through_at": row.get("event_covered_through_at"),
            "latest_notice_time": row.get("latest_notice_time"),
        }
        summaries.append(summary)
        recommendations.append(recommendation)
        strengths_col.append(_json_list(strengths[:6]))
        risks_col.append(_json_list(risks[:6]))
        event_detail_col.append(json.dumps(event_detail, ensure_ascii=False))

    out = df.copy()
    out["summary"] = summaries
    out["recommendation"] = recommendations
    out["strengths"] = strengths_col
    out["risks"] = risks_col
    out["event_risk_detail"] = event_detail_col
    return out


def _none_if_nan(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer, np.floating)):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value
    return value


def apply_canonical_execution_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """Persist only the complete, QMT-attested conservative V4 projection."""

    out = df.copy()
    evidence = (
        out["chase_evidence_status"].fillna("DATA_BLOCKED")
        .astype(str)
        .str.upper()
        if "chase_evidence_status" in out.columns
        else pd.Series("DATA_BLOCKED", index=out.index, dtype="object")
    )
    evidence = evidence.where(
        evidence.isin({"ALLOW", "BLOCK", "DATA_BLOCKED"}),
        "DATA_BLOCKED",
    )
    # No latest-row shortcut may upgrade an incomplete historical window.
    out["chase_risk_status"] = evidence
    out["ordinary_buy_eligible"] = (evidence == "ALLOW").astype(int)
    return out


def build_analysis_rows(df: pd.DataFrame, trade_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    score_cols = [
        "long_term_score", "fundamental_score", "growth_score", "valuation_score", "risk_score",
        "short_term_score", "capital_score", "technical_score", "sentiment_score", "event_score",
        "event_risk_score",
    ]
    for row in df.to_dict(orient="records"):
        latest_notice = row.get("latest_notice_time")
        if latest_notice is None or pd.isna(latest_notice) or str(latest_notice).lower() == "nan":
            last_news_time = None
        else:
            last_news_time = str(latest_notice)[:26].replace("T", " ")
        item = {
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "stock_name": str(row.get("short_name") or "")[:20],
            "analysis_date": trade_date,
            "last_news_time": last_news_time,
            "flow_trade_date": _none_if_nan(row.get("flow_trade_date")) or None,
            "hot_trade_date": _none_if_nan(row.get("hot_trade_date")) or None,
            "event_risk_level": str(row.get("event_risk_level") or "LOW"),
            "event_risk_detail": row.get("event_risk_detail") or "[]",
            "recommend_status": str(row.get("recommend_status") or "SUSPENDED"),
            "recommend_reason": str(row.get("recommend_reason") or "")[:500],
            "summary": str(row.get("summary") or "")[:500],
            "recommendation": str(row.get("recommendation") or "")[:500],
            "strengths": row.get("strengths") or "[]",
            "risks": row.get("risks") or "[]",
            "data_quality_score": _none_if_nan(row.get("data_quality_score")),
            "data_quality_flags": row.get("data_quality_flags") or "[]",
            "model_version": row.get("model_version") or MODEL_VERSION,
        }
        for col in score_cols:
            item[col] = _none_if_nan(row.get(col))
        rows.append(item)
    return rows


def build_recommendation_rows(df: pd.DataFrame, trade_date: str, top_n: int, min_score: float) -> list[dict[str, Any]]:
    stock_code_series = df["stock_code"].astype(str).str.strip().str.zfill(6)
    main_wave_signal_series = (
        df["main_wave_signal"].fillna("").astype(str)
        if "main_wave_signal" in df.columns
        else pd.Series("", index=df.index)
    )
    recommend_status_series = (
        df["recommend_status"].fillna("BLOCK").astype(str).str.upper()
    )
    event_risk_series = (
        df["event_risk_level"].fillna("DATA_BLOCKED").astype(str).str.upper()
    )
    signal_status_series = (
        df["signal_status"].fillna("BLOCK").astype(str).str.upper()
    )
    # The recommendation table is also the ranked observation ledger.  Keep
    # soft-risk SUSPENDED/WATCH rows visible for later V4/V5/V6 evaluation,
    # while hard blocks and exit signals remain excluded.  Execution remains
    # fail-closed in the selector and execution router.
    ranked_observation_candidate = (
        recommend_status_series.isin({"ALLOW", "SUSPENDED"})
        & (_numeric_col(df, "quality_score", 0.0) >= float(min_score))
    )
    main_wave_candidate = (
        (_numeric_col(df, "main_wave_score", 0.0) >= float(STRATEGY_PROFILES["main_wave"]["min_score"]))
        & recommend_status_series.isin({"ALLOW", "SUSPENDED"})
        & (event_risk_series != "CRITICAL")
    )
    eligible = df[
        (
            ranked_observation_candidate
            | main_wave_candidate
        )
        & (recommend_status_series != "BLOCK")
        & (signal_status_series != "BLOCK")
        & (~main_wave_signal_series.isin(["REDUCE", "SELL_ALERT"]))
        & (event_risk_series != "CRITICAL")
        & (stock_code_series.str.match(r"^(0|3|6)"))
    ].copy()
    eligible["ranking_score"] = pd.concat([
        _numeric_col(eligible, "final_trade_score", 0.0),
        _numeric_col(eligible, "main_wave_score", 0.0),
    ], axis=1).max(axis=1)
    eligible["stock_code"] = (
        eligible["stock_code"].astype(str).str.strip().str.zfill(6)
    )
    eligible = eligible.sort_values(
        [
            "ranking_score",
            "final_trade_score",
            "main_wave_score",
            "entry_score",
            "quality_score",
            "capital_score",
            "stock_code",
        ],
        ascending=[False, False, False, False, False, False, True],
        kind="mergesort",
    ).head(int(top_n))

    rows: list[dict[str, Any]] = []
    for row in eligible.to_dict(orient="records"):
        turnover_evidence_json = row.get("turnover_evidence_json")
        if not isinstance(turnover_evidence_json, str) or not turnover_evidence_json:
            turnover_evidence_json = build_turnover_evidence({
                "status": "DATA_BLOCKED",
                "stock_code": str(row.get("stock_code") or "").zfill(6),
                "trade_date": trade_date,
                "decision_known_at": f"{trade_date} 23:59:59",
                "source_table": "st_market_field_capture_row",
                "reason": "DATA_BLOCKED: turnover evidence not propagated",
            })
        upper_limit_evidence_json = row.get("upper_limit_evidence_json")
        upper_limit_pass = False
        if isinstance(upper_limit_evidence_json, str) and upper_limit_evidence_json:
            try:
                upper_limit_pass = (
                    validate_upper_limit_evidence(upper_limit_evidence_json)
                    .get("status")
                    == "PASS"
                )
            except ValueError:
                upper_limit_evidence_json = None
        if not isinstance(upper_limit_evidence_json, str) or not upper_limit_evidence_json:
            upper_limit_evidence_json = build_upper_limit_evidence({
                "status": "DATA_BLOCKED",
                "stock_code": str(row.get("stock_code") or "").zfill(6),
                "trade_date": trade_date,
                "decision_known_at": f"{trade_date} 23:59:59",
                "source_table": "st_market_field_capture_row",
                "reason": "DATA_BLOCKED: upper-limit evidence not propagated",
            })
            upper_limit_pass = False
        chase_risk_status = str(
            row.get("chase_risk_status") or "DATA_BLOCKED"
        ).upper()
        ordinary_buy_eligible = bool(
            row.get("ordinary_buy_eligible") is True
            or row.get("ordinary_buy_eligible") == 1
        )
        if not upper_limit_pass:
            chase_risk_status = "DATA_BLOCKED"
            ordinary_buy_eligible = False
        candidate_recommend_status = str(
            row.get("recommend_status") or "BLOCK"
        ).strip().upper()
        signal_status = str(
            row.get("signal_status") or "WATCH"
        ).strip().upper()
        membership_carry_forward = bool(
            row.get("industry_previous_session_fallback")
        )
        four_gate_executable = bool(
            candidate_recommend_status == "ALLOW"
            and signal_status in {"BUY_READY", "CONFIRM"}
            and chase_risk_status == "ALLOW"
            and ordinary_buy_eligible
            and not membership_carry_forward
        )
        if not four_gate_executable:
            # A ranked row can remain visible as a research candidate when
            # its execution evidence is incomplete, but no legacy consumer
            # may mistake that row for an ALLOW recommendation.  Persist both
            # mutable execution gates fail-closed; scheduler activation can
            # only mirror these hash-bound candidate values.
            candidate_recommend_status = "SUSPENDED"
            ordinary_buy_eligible = False
        reason = (
            f"综合{row.get('ai_score'):.1f}: "
            f"短线{row.get('short_term_score'):.1f}/长线{row.get('long_term_score'):.1f}; "
            f"{row.get('recommend_reason')}"
        )
        sources = "fast_eod"
        if row.get("source_flag") and not pd.isna(row.get("source_flag")):
            sources += f"+hot:{row.get('source_flag')}"
        rows.append({
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "ranking_score": round(float(row.get("ranking_score") or 0), 1),
            "short_name": str(row.get("short_name") or "")[:20],
            "ai_score": round(float(row.get("ai_score") or 0), 1),
            "long_term_score": round(float(row.get("long_term_score") or 0), 1),
            "short_term_score": round(float(row.get("short_term_score") or 0), 1),
            "fundamental": round(float(row.get("fundamental_score") or 0), 1),
            "capital_score": round(float(row.get("capital_score") or 0), 1),
            "valuation": round(float(row.get("valuation_score") or 0), 1),
            "technical": round(float(row.get("technical_score") or 0), 1),
            "reason": reason[:500],
            "sources": sources[:100],
            "pick_date": trade_date,
            "recommend_status": candidate_recommend_status,
            "recommend_reason": str(row.get("recommend_reason") or "")[:500],
            "candidate_recommend_status": candidate_recommend_status,
            "chase_risk_status": chase_risk_status,
            "ordinary_buy_eligible": 1 if ordinary_buy_eligible else 0,
            "candidate_ordinary_buy_eligible": 1 if ordinary_buy_eligible else 0,
            "event_risk_level": row.get("event_risk_level") or "LOW",
            "sentiment_score": round(float(row.get("sentiment_score") or 0), 1),
            "market_mood_score": round(float(row.get("market_mood_score") or 0), 1),
            "event_score": round(float(row.get("event_score") or 0), 1),
            "ultra_short_score": round(float(row.get("ultra_short_score") or 0), 1),
            "swing_score": round(float(row.get("swing_score") or 0), 1),
            "primary_strategy": row.get("primary_strategy") or "",
            "strategy_profile": row.get("strategy_profile") or "",
            "suitable_strategies": row.get("suitable_strategies") or "[]",
            "signal_status": signal_status,
            "signal_reason": str(row.get("signal_reason") or "")[:500],
            "entry_price_low": _none_if_nan(row.get("entry_price_low")),
            "entry_price_high": _none_if_nan(row.get("entry_price_high")),
            "stop_loss_price": _none_if_nan(row.get("stop_loss_price")),
            "take_profit_1": _none_if_nan(row.get("take_profit_1")),
            "take_profit_2": _none_if_nan(row.get("take_profit_2")),
            "position_weight": _none_if_nan(row.get("position_weight")),
            "max_holding_days": int(row.get("max_holding_days") or 0),
            "entry_conditions_json": row.get("entry_conditions_json") or "[]",
            "sell_rules_json": row.get("sell_rules_json") or "[]",
            "invalidation_reason": str(row.get("invalidation_reason") or "")[:500],
            "quality_score": round(float(row.get("quality_score") or 0), 1),
            "entry_score": round(float(row.get("entry_score") or 0), 1),
            "final_trade_score": round(float(row.get("final_trade_score") or 0), 1),
            "expected_return_score": round(float(row.get("expected_return_score") or 0), 1),
            "expected_return_pct": round(float(row.get("expected_return_pct") or 0), 2),
            "resistance_price": _none_if_nan(row.get("resistance_price")),
            "heat_overload_score": round(float(row.get("heat_overload_score") or 0), 1),
            "confidence_score": round(float(row.get("confidence_score") or 0), 1),
            "sector_rotation_score": round(float(row.get("sector_rotation_score") or 0), 1),
            "failure_penalty_score": round(float(row.get("failure_penalty_score") or 0), 1),
            "data_quality_score": round(float(row.get("data_quality_score") or 0), 1),
            "data_quality_flags": row.get("data_quality_flags") or "[]",
            "cooldown_days_left": int(row.get("cooldown_days_left") or 0),
            "cooldown_until": _none_if_nan(row.get("cooldown_until")),
            "main_wave_score": round(float(row.get("main_wave_score") or 0), 1),
            "trend_hold_score": round(float(row.get("trend_hold_score") or 0), 1),
            "main_wave_stage": row.get("main_wave_stage") or "",
            "main_wave_signal": row.get("main_wave_signal") or "",
            "main_wave_reason": str(row.get("main_wave_reason") or "")[:500],
            "trend_stop_price": _none_if_nan(row.get("trend_stop_price")),
            "trend_reduce_price": _none_if_nan(row.get("trend_reduce_price")),
            "model_version": row.get("model_version") or MODEL_VERSION,
            "membership_snapshot_date": row.get("industry_snapshot_date"),
            "membership_snapshot_source": str(
                row.get("industry_snapshot_source") or ""
            ),
            "membership_proof_sha256": str(
                row.get("membership_proof_sha256") or ""
            ).lower(),
            "pit_common_receipt_root_hash": str(
                row.get("pit_common_receipt_root_hash") or ""
            ).lower(),
            "finance_manifest_hash": str(
                row.get("finance_manifest_hash") or ""
            ).lower(),
            "event_manifest_hash": str(
                row.get("event_manifest_hash") or ""
            ).lower(),
            **{
                field: row.get(field)
                for field in (
                    "turnover_snapshot_run_id",
                    "turnover_snapshot_semantic_sha256",
                    "turnover_authority_identity",
                    "turnover_authority_sha256",
                    "turnover_authority_set_sha256",
                    "turnover_collector_build_sha",
                    "turnover_collector_binary_sha256",
                    "turnover_full_market_count",
                    "turnover_full_market_proof_root_sha256",
                    "flow_input_root_sha256",
                    "flow_input_count",
                    "flow_input_min_etl_sync_at",
                    "flow_input_max_etl_sync_at",
                    "flow_input_decision_at",
                )
            },
            "turnover_evidence_json": turnover_evidence_json,
            "upper_limit_evidence_json": upper_limit_evidence_json,
            "chase_bar_window_root_sha256": str(
                row.get("chase_bar_window_root_sha256") or ""
            ).lower(),
        })
    return rows


_MULTI_VALUES_BIND = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_MULTI_VALUES_DEFAULT_ROWS = 75
_MULTI_VALUES_MAX_ROWS = 100
_MULTI_VALUES_MAX_BYTES = 1_500_000


def _split_insert_values_template(sql: str) -> tuple[str, str, str]:
    """Split one INSERT into the head, first VALUES tuple, and tail.

    The daily writer owns the two static INSERT statements below.  Parsing the
    first balanced VALUES tuple lets us issue real multi-row INSERTs without
    duplicating either large column contract in Python.
    """

    match = re.search(r"\bVALUES\s*\(", str(sql or ""), flags=re.IGNORECASE)
    if match is None:
        raise ValueError("multi-values writer requires an INSERT VALUES tuple")
    opening = match.end() - 1
    depth = 0
    closing = -1
    for index in range(opening, len(sql)):
        character = sql[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                closing = index
                break
    if closing < 0:
        raise ValueError("multi-values writer VALUES tuple is unbalanced")
    values_template = sql[opening : closing + 1]
    if not _MULTI_VALUES_BIND.search(values_template):
        raise ValueError("multi-values writer VALUES tuple has no bind parameters")
    return sql[:opening], values_template, sql[closing + 1 :]


def _bound_value_size(value: Any) -> int:
    """Conservatively estimate the encoded payload added to one SQL packet."""

    if value is None:
        return 4
    if isinstance(value, memoryview):
        return (len(value) * 2) + 16
    if isinstance(value, bytes):
        return (len(value) * 2) + 16
    if isinstance(value, str):
        return (len(value.encode("utf-8")) * 2) + 2
    return len(str(value).encode("utf-8"))


def _execute_batches(
    conn,
    sql: str,
    rows: list[dict[str, Any]],
    chunk_size: int = _MULTI_VALUES_DEFAULT_ROWS,
    max_statement_bytes: int = _MULTI_VALUES_MAX_BYTES,
) -> None:
    """Write bounded multi-VALUES statements inside the caller transaction.

    PyMySQL executemany was replaced by row-wise execution after it stalled on
    JSON-heavy rows.  That workaround turned the full-market publication into
    roughly 5,200 network round trips.  Explicit multi-VALUES SQL keeps one
    execute per 50-100-row/size-bounded packet while preserving the surrounding
    fail-closed transaction and its complete database readback.
    """

    if not rows:
        return
    row_limit = max(1, min(_MULTI_VALUES_MAX_ROWS, int(chunk_size)))
    byte_limit = max(64 * 1024, min(2_000_000, int(max_statement_bytes)))
    head, values_template, tail = _split_insert_values_template(sql)
    bind_names = tuple(_MULTI_VALUES_BIND.findall(values_template))
    unique_bind_names = frozenset(bind_names)
    fixed_bytes = len((head + tail).encode("utf-8"))

    pending_values: list[str] = []
    pending_params: dict[str, Any] = {}
    pending_bytes = fixed_bytes

    def flush() -> None:
        nonlocal pending_values, pending_params, pending_bytes
        if not pending_values:
            return
        statement = head + ",\n".join(pending_values) + tail
        conn.execute(text(statement), pending_params)
        pending_values = []
        pending_params = {}
        pending_bytes = fixed_bytes

    for source in rows:
        row = dict(source)
        missing = sorted(unique_bind_names - set(row))
        if missing:
            raise ValueError(
                "multi-values writer row is missing binds: " + ", ".join(missing)
            )

        # Bind names only need to be unique within the current SQL statement.
        ordinal = len(pending_values)
        rendered_values = _MULTI_VALUES_BIND.sub(
            lambda match: f":mv_{ordinal}_{match.group(1)}",
            values_template,
        )
        row_params = {
            f"mv_{ordinal}_{name}": row[name]
            for name in unique_bind_names
        }
        row_bytes = len(rendered_values.encode("utf-8")) + sum(
            _bound_value_size(value) + 8 for value in row_params.values()
        )
        if pending_values and (
            len(pending_values) >= row_limit
            or pending_bytes + row_bytes > byte_limit
        ):
            flush()
            ordinal = 0
            rendered_values = _MULTI_VALUES_BIND.sub(
                lambda match: f":mv_{ordinal}_{match.group(1)}",
                values_template,
            )
            row_params = {
                f"mv_{ordinal}_{name}": row[name]
                for name in unique_bind_names
            }
            row_bytes = len(rendered_values.encode("utf-8")) + sum(
                _bound_value_size(value) + 8 for value in row_params.values()
            )
        if fixed_bytes + row_bytes > byte_limit:
            raise ValueError(
                "multi-values writer row exceeds the SQL packet byte limit"
            )
        pending_values.append(rendered_values)
        pending_params.update(row_params)
        pending_bytes += row_bytes
    flush()


def _normalize_stock_codes(stock_codes: list[str] | None) -> list[str]:
    if not stock_codes:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for code in stock_codes:
        value = str(code or "").strip().zfill(6)
        if value and value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _filter_frame_by_codes(df: pd.DataFrame, stock_codes: list[str] | None) -> pd.DataFrame:
    codes = _normalize_stock_codes(stock_codes)
    if not codes or df is None or df.empty or "stock_code" not in df.columns:
        return df
    return df[df["stock_code"].astype(str).str.strip().str.zfill(6).isin(codes)].copy()


def _delete_scope_rows(
    conn,
    table_name: str,
    date_column: str,
    trade_date: str,
    stock_codes: list[str] | None = None,
) -> None:
    codes = _normalize_stock_codes(stock_codes)
    if not codes:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE {date_column} = :trade_date"),
            {"trade_date": trade_date},
        )
        return

    placeholders = ", ".join(f":code_{idx}" for idx in range(len(codes)))
    params = {"trade_date": trade_date, **{f"code_{idx}": code for idx, code in enumerate(codes)}}
    conn.execute(
        text(
            f"DELETE FROM {table_name} "
            f"WHERE {date_column} = :trade_date AND stock_code IN ({placeholders})"
        ),
        params,
    )


def save_outputs(
    engine: Engine,
    analysis_rows: list[dict[str, Any]],
    rec_rows: list[dict[str, Any]],
    trade_date: str,
    stock_codes: list[str] | None = None,
    *,
    publication_run_uid: str = "",
    publisher_task_type: str = "",
    publisher_build_sha: str = "",
) -> dict[str, Any] | None:
    analysis_sql = """
        INSERT INTO stock_analysis_result (
            stock_code, stock_name, analysis_date, last_news_time,
            long_term_score, fundamental_score, growth_score, valuation_score, risk_score,
            short_term_score, capital_score, technical_score, sentiment_score, event_score,
            event_risk_score, event_risk_level, event_risk_detail,
            recommend_status, recommend_reason,
            summary, recommendation, strengths, risks,
            data_quality_score, data_quality_flags, flow_trade_date, hot_trade_date, model_version,
            created_at, updated_at
        ) VALUES (
            :stock_code, :stock_name, :analysis_date, :last_news_time,
            :long_term_score, :fundamental_score, :growth_score, :valuation_score, :risk_score,
            :short_term_score, :capital_score, :technical_score, :sentiment_score, :event_score,
            :event_risk_score, :event_risk_level, :event_risk_detail,
            :recommend_status, :recommend_reason,
            :summary, :recommendation, :strengths, :risks,
            :data_quality_score, :data_quality_flags, :flow_trade_date, :hot_trade_date, :model_version,
            NOW(), NOW()
        )
        ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            last_news_time = VALUES(last_news_time),
            long_term_score = VALUES(long_term_score),
            fundamental_score = VALUES(fundamental_score),
            growth_score = VALUES(growth_score),
            valuation_score = VALUES(valuation_score),
            risk_score = VALUES(risk_score),
            short_term_score = VALUES(short_term_score),
            capital_score = VALUES(capital_score),
            technical_score = VALUES(technical_score),
            sentiment_score = VALUES(sentiment_score),
            event_score = VALUES(event_score),
            event_risk_score = VALUES(event_risk_score),
            event_risk_level = VALUES(event_risk_level),
            event_risk_detail = VALUES(event_risk_detail),
            recommend_status = VALUES(recommend_status),
            recommend_reason = VALUES(recommend_reason),
            summary = VALUES(summary),
            recommendation = VALUES(recommendation),
            strengths = VALUES(strengths),
            risks = VALUES(risks),
            data_quality_score = VALUES(data_quality_score),
            data_quality_flags = VALUES(data_quality_flags),
            flow_trade_date = VALUES(flow_trade_date),
            hot_trade_date = VALUES(hot_trade_date),
            model_version = VALUES(model_version),
            updated_at = NOW()
    """
    rec_sql = """
        INSERT INTO st_recommended_stocks (
            stock_code, short_name, ai_score, long_term_score, short_term_score,
            fundamental, capital_score, valuation, technical,
            reason, sources, pick_date,
            recommend_status, recommend_reason, candidate_recommend_status,
            chase_risk_status, ordinary_buy_eligible,
            candidate_ordinary_buy_eligible, publisher_run_uid,
            publication_status, membership_snapshot_date,
            membership_snapshot_source, membership_proof_sha256,
            turnover_evidence_json, upper_limit_evidence_json,
            event_risk_level,
            last_check_time, sentiment_score, market_mood_score, event_score,
            ultra_short_score, swing_score, primary_strategy, strategy_profile,
            suitable_strategies, signal_status, signal_reason,
            entry_price_low, entry_price_high, stop_loss_price,
            take_profit_1, take_profit_2, position_weight, max_holding_days,
            entry_conditions_json, sell_rules_json, invalidation_reason,
            quality_score, entry_score, final_trade_score,
            expected_return_score, expected_return_pct, resistance_price,
            heat_overload_score, confidence_score, sector_rotation_score,
            failure_penalty_score, data_quality_score, data_quality_flags, cooldown_days_left, cooldown_until,
            main_wave_score, trend_hold_score, main_wave_stage, main_wave_signal,
            main_wave_reason, trend_stop_price, trend_reduce_price,
            model_version,
            created_at
        ) VALUES (
            :stock_code, :short_name, :ai_score, :long_term_score, :short_term_score,
            :fundamental, :capital_score, :valuation, :technical,
            :reason, :sources, :pick_date,
            :recommend_status, :recommend_reason, :candidate_recommend_status,
            :chase_risk_status, :ordinary_buy_eligible,
            :candidate_ordinary_buy_eligible, :publisher_run_uid,
            :publication_status, :membership_snapshot_date,
            :membership_snapshot_source, :membership_proof_sha256,
            :turnover_evidence_json, :upper_limit_evidence_json,
            :event_risk_level,
            NOW(), :sentiment_score, :market_mood_score, :event_score,
            :ultra_short_score, :swing_score, :primary_strategy, :strategy_profile,
            :suitable_strategies, :signal_status, :signal_reason,
            :entry_price_low, :entry_price_high, :stop_loss_price,
            :take_profit_1, :take_profit_2, :position_weight, :max_holding_days,
            :entry_conditions_json, :sell_rules_json, :invalidation_reason,
            :quality_score, :entry_score, :final_trade_score,
            :expected_return_score, :expected_return_pct, :resistance_price,
            :heat_overload_score, :confidence_score, :sector_rotation_score,
            :failure_penalty_score, :data_quality_score, :data_quality_flags, :cooldown_days_left, :cooldown_until,
            :main_wave_score, :trend_hold_score, :main_wave_stage, :main_wave_signal,
            :main_wave_reason, :trend_stop_price, :trend_reduce_price,
            :model_version,
            NOW()
        )
    """
    scoped_codes = _normalize_stock_codes(stock_codes)
    identity_values = (
        str(publication_run_uid or "").strip().lower(),
        str(publisher_task_type or "").strip(),
        str(publisher_build_sha or "").strip().lower(),
    )
    publish = any(identity_values)
    production = (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    )
    if production and not publish:
        raise RuntimeError(
            "production analysis output writer requires an exact canonical "
            "publication identity"
        )
    if publish:
        run_uid, task_type, build_sha = identity_values
        if (
            not all(identity_values)
            or re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
            or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
            or build_sha == "0" * 40
            or task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        ):
            raise RuntimeError("analysis publication identity is invalid")
        if scoped_codes:
            raise RuntimeError(
                "scoped analysis cannot publish the full daily partition"
            )
        validate_recommended_run_history_schema(engine)
    validate_analysis_output_schema(engine)
    write_rec_rows: list[dict[str, Any]] = []
    for source in rec_rows:
        row = dict(source)
        candidate_status = str(
            row.get("candidate_recommend_status")
            or row.get("recommend_status")
            or "BLOCK"
        ).strip().upper()
        candidate_ordinary = 1 if (
            row.get("candidate_ordinary_buy_eligible") is True
            or row.get("candidate_ordinary_buy_eligible") == 1
            or row.get("ordinary_buy_eligible") is True
            or row.get("ordinary_buy_eligible") == 1
        ) else 0
        row.update({
            "candidate_recommend_status": candidate_status,
            "candidate_ordinary_buy_eligible": candidate_ordinary,
            "publisher_run_uid": run_uid if publish else "",
            "publication_status": "PENDING" if publish else "ACTIVE",
        })
        if publish:
            # The mutable pool is committed fail-closed.  Scheduler
            # postvalidation later activates these two already-ubiquitous
            # execution gates together with publication_status.
            row["recommend_status"] = "PENDING"
            row["ordinary_buy_eligible"] = 0
        else:
            row["recommend_status"] = candidate_status
            row["ordinary_buy_eligible"] = candidate_ordinary
        write_rec_rows.append(row)
    with engine.begin() as conn:
        logger.info("Writing %s analysis rows for %s", len(analysis_rows), trade_date)
        _delete_scope_rows(
            conn,
            table_name="stock_analysis_result",
            date_column="analysis_date",
            trade_date=trade_date,
            stock_codes=scoped_codes,
        )
        _execute_batches(conn, analysis_sql, analysis_rows)
        logger.info("Refreshing %s recommendation rows for %s", len(rec_rows), trade_date)
        _delete_scope_rows(
            conn,
            table_name="st_recommended_stocks",
            date_column="pick_date",
            trade_date=trade_date,
            stock_codes=scoped_codes,
        )
        if write_rec_rows:
            _execute_batches(conn, rec_sql, write_rec_rows)
        if not publish:
            return None

        manifest = read_persisted_pool_manifest(conn, trade_date)
        if (
            int(manifest["analysis_count"]) != len(analysis_rows)
            or int(manifest["recommendation_count"]) != len(write_rec_rows)
        ):
            raise RuntimeError(
                "analysis publication readback count differs from written rows"
            )
        if (
            int(manifest["executable_count"]) <= 0
            and not research_only_publication_is_safe(manifest)
        ):
            raise RuntimeError(
                "analysis publication has neither a four-gate executable "
                "strategy pool nor a sealed research-only pool"
            )
        membership_proofs = manifest.get("membership_proofs")
        membership = (
            dict(membership_proofs[0])
            if isinstance(membership_proofs, list)
            and len(membership_proofs) == 1
            and isinstance(membership_proofs[0], Mapping)
            else {}
        )
        membership_hash = str(
            membership.get("proof_sha256") or ""
        ).lower()
        if (
            manifest.get("publisher_run_uids") != [run_uid]
            or manifest.get("publication_statuses") != ["PENDING"]
            or manifest.get("live_gate_alignment") is not True
            or membership.get("snapshot_date") != trade_date
            or membership.get("source") != "gj_big_qmt_inner"
            or re.fullmatch(
                r"[0-9a-f]{64}",
                membership_hash,
            ) is None
        ):
            raise RuntimeError(
                "analysis publication staging identity differs from writer"
            )
        dialect_name = str(
            getattr(getattr(engine, "dialect", None), "name", "")
        ).lower()
        if dialect_name == "mysql":
            published_at = conn.execute(
                # st_recommended_run_history.started_at/finished_at use
                # second-precision DATETIME/NOW.  Source publication time
                # from the same database clock and precision so same-second
                # publish/finish ordering remains comparable.
                text("SELECT CURRENT_TIMESTAMP")
            ).scalar()
            if not isinstance(published_at, datetime):
                raise RuntimeError(
                    "analysis publication database timestamp is unavailable"
                )
        else:
            published_at = _now_shanghai_naive()
        result = conn.execute(text("""
            UPDATE st_recommended_run_history
            SET publisher_task_type=:publisher_task_type,
                canonical_pool_sha256=:canonical_pool_sha256,
                published_at=:published_at,
                executable_count=:executable_count,
                membership_snapshot_date=:membership_snapshot_date,
                membership_snapshot_source=:membership_snapshot_source,
                membership_proof_sha256=:membership_proof_sha256,
                total=:analysis_count,
                passed=:recommendation_count
            WHERE run_uid=:run_uid
              AND scheduler_job_id=:run_uid
              AND trade_date=:trade_date
              AND build_sha=:build_sha
              AND status='running'
              AND canonical_pool_sha256 IS NULL
              AND published_at IS NULL
        """), {
            "publisher_task_type": task_type,
            "canonical_pool_sha256": manifest["canonical_pool_sha256"],
            "published_at": published_at,
            "executable_count": int(manifest["executable_count"]),
            "membership_snapshot_date": membership["snapshot_date"],
            "membership_snapshot_source": membership["source"],
            "membership_proof_sha256": membership_hash,
            "analysis_count": int(manifest["analysis_count"]),
            "recommendation_count": int(manifest["recommendation_count"]),
            "run_uid": run_uid,
            "trade_date": trade_date,
            "build_sha": build_sha,
        })
        if int(result.rowcount or 0) != 1:
            raise RuntimeError(
                "analysis publication history was not bound exactly once"
            )
        history_rows = conn.execute(text("""
            SELECT publisher_task_type, canonical_pool_sha256, published_at,
                   executable_count, membership_snapshot_date,
                   membership_snapshot_source, membership_proof_sha256,
                   total, passed
            FROM st_recommended_run_history
            WHERE run_uid=:run_uid
            LIMIT 2
        """), {"run_uid": run_uid}).mappings().all()
        if len(history_rows) != 1:
            raise RuntimeError("analysis publication history readback is ambiguous")
        history = history_rows[0]
        if (
            str(history.get("publisher_task_type") or "") != task_type
            or str(history.get("canonical_pool_sha256") or "").lower()
            != manifest["canonical_pool_sha256"]
            or int(history.get("executable_count") or 0)
            != int(manifest["executable_count"])
            or int(history.get("total") or 0) != int(manifest["analysis_count"])
            or int(history.get("passed") or 0)
            != int(manifest["recommendation_count"])
            or str(history.get("membership_snapshot_date") or "")[:10]
            != membership["snapshot_date"]
            or str(history.get("membership_snapshot_source") or "")
            != membership["source"]
            or str(history.get("membership_proof_sha256") or "").lower()
            != membership_hash
            or history.get("published_at") is None
        ):
            raise RuntimeError("analysis publication history readback differs")
        return build_publication_receipt(
            manifest=manifest,
            run_uid=run_uid,
            publisher_task_type=task_type,
            build_sha=build_sha,
            published_at=history["published_at"],
        )


def _emit_progress(progress_callback: ProgressCallback | None, **payload: Any) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(payload)
    except Exception:
        logger.debug("progress callback failed", exc_info=True)


def _refresh_exact_upper_limit_execution_evidence(
    *,
    engine: Engine,
    scored: pd.DataFrame,
    trade_date: str,
    decision_at: datetime | str | None,
    top_n: int,
    min_score: float,
    flow_date: str,
    publisher_build_sha: str,
) -> pd.DataFrame:
    """Recompute Frozen V4 only when one exact top-80 capture is available."""

    preliminary_rec_rows = build_recommendation_rows(
        scored, trade_date, top_n=top_n, min_score=min_score
    )
    preliminary_bar_roots = {
        str(row.get("stock_code") or "").strip(): str(
            row.get("chase_bar_window_root_sha256") or ""
        ).strip().lower()
        for row in preliminary_rec_rows
    }
    preliminary_codes = sorted(
        str(row.get("stock_code") or "").strip()
        for row in preliminary_rec_rows
    )
    if (
        decision_at is None
        or int(top_n) != 80
        or len(preliminary_codes) != 80
        or len(set(preliminary_codes)) != 80
        or set(preliminary_bar_roots) != set(preliminary_codes)
        or any(
            re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in preliminary_bar_roots.values()
        )
    ):
        return scored
    preliminary_receipt = build_preliminary_upper_subject_receipt(
        trade_date=trade_date,
        decision_at=decision_at,
        build_sha=publisher_build_sha,
        model_version=MODEL_VERSION,
        min_score=min_score,
        candidates=preliminary_rec_rows,
    )
    validate_preliminary_upper_subject_receipt(preliminary_receipt)
    upper_snapshot = load_latest_verified_upper_limit_evidence(
        engine,
        target_date=trade_date,
        decision_at=decision_at,
        stock_codes=preliminary_codes,
        preliminary_receipt_sha256=preliminary_receipt["receipt_sha256"],
        preliminary_build_sha=publisher_build_sha,
    )
    if not upper_snapshot:
        return scored
    chase_dates = _recent_dates(
        engine,
        "sm_stock_kline",
        "trade_date",
        trade_date,
        90,
        known_at_column="received_at",
        decision_known_at=decision_at,
    )
    if not chase_dates:
        raise RuntimeError(
            "upper-limit recomputation has no knowledge-aware bars"
        )
    refreshed = _load_canonical_chase_risk_evidence(
        engine,
        start_date=chase_dates[-1],
        trade_date=trade_date,
        decision_known_at=decision_at,
        upper_limit_evidence=upper_snapshot,
        stock_codes=preliminary_codes,
    )
    refreshed_codes = refreshed[refreshed["stock_code"].isin(preliminary_codes)].copy()
    if (
        len(refreshed_codes) != len(preliminary_codes)
        or refreshed_codes["stock_code"].duplicated().any()
    ):
        raise RuntimeError(
            "upper-limit recomputation exact chase bar subject differs"
        )
    refreshed_bar_roots = dict(zip(
        refreshed_codes["stock_code"].astype(str),
        refreshed_codes["chase_bar_window_root_sha256"].astype(str).str.lower(),
    ))
    if refreshed_bar_roots != preliminary_bar_roots:
        raise RuntimeError(
            "upper-limit recomputation chase bar evidence changed after preview"
        )
    refreshed = refreshed.rename(columns={
        "turnover_ratio_effective": "turnover_ratio",
    })
    refreshed_columns = [
        column for column in refreshed.columns if column != "stock_code"
    ]
    result = scored.drop(
        columns=[
            column for column in refreshed_columns
            if column in scored.columns
        ],
        errors="ignore",
    ).merge(refreshed, on="stock_code", how="left")
    result = apply_canonical_execution_eligibility(result)
    return _build_text_fields(
        result, flow_date=flow_date, trade_date=trade_date
    )


def _prepare_batch_outputs(
    engine: Engine,
    trade_date: str,
    min_score: float,
    top_n: int,
    stock_codes: list[str] | None = None,
    progress_callback: ProgressCallback | None = None,
    news_cutoff_time: str | None = None,
    publisher_build_sha: str = "",
    refresh_upper_limit: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float, str, str]:
    scoped_codes = _normalize_stock_codes(stock_codes)
    decision_at: datetime | str | None = (
        normalize_decision_at(news_cutoff_time)
        if news_cutoff_time is not None
        else None
    )

    _emit_progress(progress_callback, stage="load_kline", percent=5, step="加载日K特征...", trade_date=trade_date)
    kline_all = load_kline_features(
        engine,
        trade_date,
        decision_known_at=decision_at,
        progress_callback=progress_callback,
    )
    kline = _filter_frame_by_codes(kline_all, scoped_codes)
    if scoped_codes and kline.empty:
        raise RuntimeError(f"No K-line rows found for requested stock codes on {trade_date}")
    feature_codes = (
        sorted(
            kline["stock_code"].astype(str).str.strip().str.zfill(6).unique()
        )
        if "stock_code" in kline.columns
        else []
    )
    turnover_input_proof: dict[str, Any] = {}
    if decision_at is not None:
        turnover_input_proof = _verify_full_market_turnover_inputs(
            kline_all,
            trade_date=trade_date,
        )
    common_cutoff: dict[str, Any] = {
        "status": PIT_DATA_BLOCKED,
        "reason": "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED",
        "fact_cutoff_at": "",
        "receipt_root_hash": "",
    }
    if decision_at is not None:
        common_cutoff = resolve_common_fact_cutoff(
            engine,
            codes=feature_codes,
            decision_at=decision_at,
            finance_start_date="1900-01-01",
            finance_end_date=trade_date,
            event_start_date=(
                date.fromisoformat(trade_date) - timedelta(days=14)
            ),
            event_end_date=trade_date,
            require_qmt_event_batch=True,
        )
    fact_cutoff_at: datetime | str | None = (
        common_cutoff.get("fact_cutoff_at")
        if common_cutoff.get("status") == PIT_AVAILABLE
        else None
    )
    reader_decision_at = (
        decision_at
        if common_cutoff.get("status") == PIT_AVAILABLE
        else None
    )

    _emit_progress(progress_callback, stage="load_finance", percent=14, step="加载财务因子...", trade_date=trade_date)
    finance = _filter_frame_by_codes(
        load_finance(
            engine,
            trade_date,
            decision_at=reader_decision_at,
            fact_cutoff_at=fact_cutoff_at,
            stock_codes=feature_codes,
        ),
        scoped_codes,
    )
    finance["pit_common_cutoff_status"] = common_cutoff.get("status")
    finance["pit_common_cutoff_reason"] = common_cutoff.get("reason") or ""
    finance["pit_common_receipt_root_hash"] = (
        common_cutoff.get("receipt_root_hash") or ""
    )
    if common_cutoff.get("status") != PIT_AVAILABLE:
        finance["finance_pit_status"] = PIT_DATA_BLOCKED
        finance["finance_pit_reason"] = common_cutoff.get("reason")
    _emit_progress(progress_callback, stage="load_flow", percent=23, step="加载资金流数据...", trade_date=trade_date)
    flow_all, flow_date = load_flow_features(
        engine,
        trade_date,
        decision_known_at=decision_at,
    )
    flow_input_proof = validate_exact_daily_flow_coverage(
        engine,
        trade_date=trade_date,
        kline=kline_all,
        flow=flow_all,
        decision_known_at=(decision_at if isinstance(decision_at, datetime) else None),
    )
    flow = _filter_frame_by_codes(flow_all, scoped_codes)
    _emit_progress(progress_callback, stage="load_hot", percent=32, step="加载热度排行...", trade_date=trade_date)
    hot, hot_date = load_hot_rank(
        engine,
        trade_date,
        decision_at=decision_at,
    )
    hot = _filter_frame_by_codes(hot, scoped_codes)
    _emit_progress(progress_callback, stage="load_notices", percent=40, step="加载公告事件...", trade_date=trade_date)
    notices = _filter_frame_by_codes(
        load_notice_features(
            engine,
            trade_date,
            decision_at=reader_decision_at,
            fact_cutoff_at=fact_cutoff_at,
            stock_codes=feature_codes,
        ),
        scoped_codes,
    )
    if common_cutoff.get("status") != PIT_AVAILABLE:
        notices["event_pit_status"] = PIT_DATA_BLOCKED
        notices["event_pit_reason"] = common_cutoff.get("reason")
    # ``st_news_flash`` has no immutable received/revision history.  It remains
    # available for display, but cannot alter strategy event scores until it is
    # migrated to the shared PIT contract.
    news = pd.DataFrame({"stock_code": []})
    notices = merge_event_features(notices, news)
    _emit_progress(progress_callback, stage="load_confidence", percent=48, step="加载交易置信度...", trade_date=trade_date)
    confidence = _filter_frame_by_codes(
        load_confidence_features(
            engine,
            trade_date,
            decision_at=decision_at,
        ),
        scoped_codes,
    )
    _emit_progress(progress_callback, stage="load_history", percent=56, step="加载历史推荐表现...", trade_date=trade_date)
    rec_history = _filter_frame_by_codes(
        load_recommendation_history(
            engine,
            trade_date,
            decision_at=decision_at,
        ),
        scoped_codes,
    )
    _emit_progress(progress_callback, stage="load_failures", percent=62, step="加载失败惩罚因子...", trade_date=trade_date)
    failures = _filter_frame_by_codes(
        load_failure_features(
            engine,
            trade_date,
            decision_at=decision_at,
        ),
        scoped_codes,
    )
    _emit_progress(progress_callback, stage="load_sector", percent=68, step="加载板块轮动因子...", trade_date=trade_date)
    sector = _filter_frame_by_codes(
        load_sector_rotation_features(
            engine,
            trade_date,
            decision_known_at=decision_at,
        ),
        scoped_codes,
    )
    sector = _complete_membership_proof_scope(sector, feature_codes)
    market_mood_score = compute_market_mood(kline_all)

    logger.info(
        "Loaded data: scope=%s kline=%s finance=%s flow=%s notices=%s hot=%s confidence=%s history=%s failures=%s sector=%s market_mood=%.1f",
        "all" if not scoped_codes else len(scoped_codes),
        len(kline), len(finance), len(flow), len(notices), len(hot),
        len(confidence), len(rec_history), len(failures), len(sector), market_mood_score,
    )

    _emit_progress(progress_callback, stage="compute_scores", percent=78, step="计算全市场评分...", trade_date=trade_date)
    scored = compute_scores(
        kline=kline,
        finance=finance,
        flow=flow,
        hot=hot,
        notices=notices,
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        trade_date=trade_date,
        min_score=min_score,
        sector=sector,
        confidence=confidence,
        rec_history=rec_history,
        failures=failures,
    )
    scored["hot_trade_date"] = hot_date
    flow_proof_fields = (
        "flow_input_root_sha256",
        "flow_input_count",
        "flow_input_min_etl_sync_at",
        "flow_input_max_etl_sync_at",
        "flow_input_decision_at",
    )
    scoring_input_proof = dict(turnover_input_proof)
    if isinstance(flow_input_proof, Mapping) and all(
        key in flow_input_proof for key in flow_proof_fields
    ):
        scoring_input_proof.update({
            key: flow_input_proof[key] for key in flow_proof_fields
        })
    elif decision_at is not None:
        raise RuntimeError("DATA_BLOCKED: formal flow input proof is unavailable")
    for field, value in scoring_input_proof.items():
        scored[field] = value
    scored = apply_canonical_execution_eligibility(scored)
    _emit_progress(progress_callback, stage="build_rows", percent=88, step="生成分析与推荐结果...", trade_date=trade_date)
    scored = _build_text_fields(scored, flow_date=flow_date, trade_date=trade_date)
    if refresh_upper_limit:
        scored = _refresh_exact_upper_limit_execution_evidence(
            engine=engine,
            scored=scored,
            trade_date=trade_date,
            decision_at=decision_at,
            top_n=top_n,
            min_score=min_score,
            flow_date=flow_date,
            publisher_build_sha=publisher_build_sha,
        )
    analysis_rows = build_analysis_rows(scored, trade_date)
    rec_rows = build_recommendation_rows(scored, trade_date, top_n=top_n, min_score=min_score)
    return analysis_rows, rec_rows, market_mood_score, flow_date, hot_date


def prepare_preliminary_upper_subject_receipt(
    engine: Engine,
    *,
    trade_date: str,
    decision_at: datetime | str,
    build_sha: str,
    min_score: float = 62.0,
) -> dict[str, Any]:
    """Read facts only and seal the deterministic ordered pre-upper top 80."""

    exact_decision = _formal_analysis_decision_at(decision_at)
    _analysis_rows, candidates, _mood, _flow_date, _hot_date = (
        _prepare_batch_outputs(
            engine=engine,
            trade_date=trade_date,
            min_score=min_score,
            top_n=80,
            stock_codes=None,
            progress_callback=None,
            news_cutoff_time=exact_decision,
            publisher_build_sha=build_sha,
            refresh_upper_limit=False,
        )
    )
    receipt = build_preliminary_upper_subject_receipt(
        trade_date=trade_date,
        decision_at=exact_decision,
        build_sha=build_sha,
        model_version=MODEL_VERSION,
        min_score=min_score,
        candidates=candidates,
    )
    return validate_preliminary_upper_subject_receipt(receipt)


def run_batch(
    engine: Engine,
    trade_date: str | None = None,
    top_n: int = 80,
    min_score: float = 62.0,
    progress_callback: ProgressCallback | None = None,
    strict_prev_trade_day: bool = False,
    execution_time: str | None = None,
    min_kline_coverage: float = 0.80,
    auto_repair_missing_kline: bool = False,
    publication_run_uid: str = "",
    publisher_task_type: str = "",
    publisher_build_sha: str = "",
) -> BatchStats:
    publication_identity = (
        str(publication_run_uid or "").strip().lower(),
        str(publisher_task_type or "").strip(),
        str(publisher_build_sha or "").strip().lower(),
    )
    publish = any(publication_identity)
    production = (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    )
    if publish:
        run_uid, task_type, build_sha = publication_identity
        if (
            not all(publication_identity)
            or re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
            or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
            or build_sha == "0" * 40
            or task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        ):
            raise RuntimeError("analysis publication identity is invalid")
    if production and not publish:
        raise RuntimeError(
            "production analysis requires an exact canonical publication identity"
        )
    if production or publish:
        # Validate before any mutable input is read.  A publisher with an
        # explicit date but no cutoff would otherwise re-enable current-state
        # hot/learning data and make its result impossible to replay.
        exact_decision_at = _formal_analysis_decision_at(execution_time)
        if _now_shanghai_naive() < exact_decision_at:
            raise RuntimeError(
                "formal analysis cannot publish before its decision cutoff"
            )
    if auto_repair_missing_kline and (
        production
    ):
        raise RuntimeError(
            "production analysis cannot repair QMT history on the Linux host"
        )
    requested_trade_date = trade_date
    if strict_prev_trade_day:
        execution_time = execution_time or _now_shanghai_naive().replace(
            microsecond=0
        ).isoformat(sep=" ")
        resolved_trade_date = previous_trade_date(engine, execution_time)
        if trade_date and str(trade_date)[:10] != resolved_trade_date:
            raise RuntimeError(
                f"Strict morning run requires previous trade date {resolved_trade_date}, "
                f"got {str(trade_date)[:10]}"
            )
        trade_date = resolved_trade_date
        try:
            readiness = assert_trade_date_ready(engine, trade_date, min_coverage=min_kline_coverage)
        except Exception as exc:
            if not auto_repair_missing_kline:
                raise
            _emit_progress(
                progress_callback,
                stage="strict_date_missing",
                percent=2,
                step=f"strict previous trade date missing; repairing before analysis: {exc}",
                trade_date=trade_date,
                error=str(exc),
            )
            repair_missing_qmt_kline_for_trade_date(
                trade_date,
                progress_callback=progress_callback,
            )
            readiness = assert_trade_date_ready(engine, trade_date, min_coverage=min_kline_coverage)
        _emit_progress(
            progress_callback,
            stage="strict_date_ready",
            percent=2,
            step="strict previous trade date ready",
            trade_date=trade_date,
            latest_kline_date=readiness.get("latest_kline_date"),
            kline_count=readiness.get("kline_count"),
            expected_count=readiness.get("expected_count"),
        )
    else:
        trade_date = trade_date or latest_trade_date(engine)
        if execution_time is None and requested_trade_date is None:
            execution_time = _now_shanghai_naive().replace(microsecond=0).isoformat(
                sep=" "
            )
    logger.info("Fast analysis batch started for %s", trade_date)
    with _analysis_execution_lock(engine, trade_date) as verify_write_owner:
        analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
            engine=engine,
            trade_date=trade_date,
            min_score=min_score,
            top_n=top_n,
            stock_codes=None,
            progress_callback=progress_callback,
            news_cutoff_time=execution_time,
            publisher_build_sha=publisher_build_sha,
        )
        _emit_progress(
            progress_callback,
            stage="save_outputs",
            percent=94,
            step="save outputs",
            trade_date=trade_date,
            analysis_count=len(analysis_rows),
            recommendation_count=len(rec_rows),
        )
        verify_write_owner()
        publication_receipt = save_outputs(
            engine,
            analysis_rows,
            rec_rows,
            trade_date,
            publication_run_uid=publication_run_uid,
            publisher_task_type=publisher_task_type,
            publisher_build_sha=publisher_build_sha,
        )

    receipt = (
        dict(publication_receipt)
        if isinstance(publication_receipt, Mapping)
        else None
    )
    executable_count = (
        int(receipt.get("executable_count") or 0)
        if receipt is not None
        else sum(1 for row in rec_rows if is_executable_recommendation(row))
    )

    stats = BatchStats(
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        hot_date=hot_date,
        executable_count=executable_count,
        canonical_pool_sha256=str(
            (receipt or {}).get("canonical_pool_sha256") or ""
        ),
        publication_receipt=receipt,
    )
    _emit_progress(
        progress_callback,
        stage="done",
        percent=100,
        step="done",
        trade_date=stats.trade_date,
        analysis_count=stats.analysis_count,
        recommendation_count=stats.recommendation_count,
        market_mood_score=stats.market_mood_score,
        flow_date=stats.flow_date,
        hot_date=stats.hot_date,
        executable_count=stats.executable_count,
        done=stats.analysis_count,
    )
    logger.info("Fast analysis completed: %s", stats)
    return stats


def run_batch_for_codes(
    engine: Engine,
    stock_codes: list[str],
    trade_date: str | None = None,
    top_n: int = 80,
    min_score: float = 62.0,
    progress_callback: ProgressCallback | None = None,
    decision_at: datetime | str | None = None,
) -> BatchStats:
    if (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    ):
        raise RuntimeError(
            "production scoped analysis cannot mutate the canonical daily pool"
        )
    scoped_codes = _normalize_stock_codes(stock_codes)
    if not scoped_codes:
        raise ValueError("stock_codes must not be empty")

    trade_date = trade_date or latest_trade_date(engine)
    logger.info("Fast scoped analysis started for %s with %s codes", trade_date, len(scoped_codes))
    with _analysis_execution_lock(engine, trade_date) as verify_write_owner:
        analysis_rows, rec_rows, market_mood_score, flow_date, hot_date = _prepare_batch_outputs(
            engine=engine,
            trade_date=trade_date,
            min_score=min_score,
            top_n=max(int(top_n), len(scoped_codes)),
            stock_codes=scoped_codes,
            progress_callback=progress_callback,
            news_cutoff_time=decision_at,
            publisher_build_sha="",
        )
        _emit_progress(
            progress_callback,
            stage="save_outputs",
            percent=94,
            step="save outputs",
            trade_date=trade_date,
            analysis_count=len(analysis_rows),
            recommendation_count=len(rec_rows),
            done=len(analysis_rows),
        )
        verify_write_owner()
        save_outputs(
            engine,
            analysis_rows,
            rec_rows,
            trade_date,
            stock_codes=scoped_codes,
        )

    stats = BatchStats(
        trade_date=trade_date,
        analysis_count=len(analysis_rows),
        recommendation_count=len(rec_rows),
        market_mood_score=market_mood_score,
        flow_date=flow_date,
        hot_date=hot_date,
        executable_count=sum(
            1 for row in rec_rows if is_executable_recommendation(row)
        ),
    )
    _emit_progress(
        progress_callback,
        stage="done",
        percent=100,
        step="done",
        trade_date=stats.trade_date,
        analysis_count=stats.analysis_count,
        recommendation_count=stats.recommendation_count,
        market_mood_score=stats.market_mood_score,
        flow_date=stats.flow_date,
        hot_date=stats.hot_date,
        executable_count=stats.executable_count,
        done=stats.analysis_count,
    )
    logger.info("Fast scoped analysis completed: %s", stats)
    return stats


def _prebind_direct_publication_history(
    engine: Engine,
    *,
    run_uid: str,
    task_type: str,
    build_sha: str,
    trade_date: str,
    min_score: float,
    top_n: int,
    strict_prev_trade_day: bool,
    execution_time: str,
) -> None:
    """Create the exact recommendation audit row for a direct scheduler CLI."""

    if (
        re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
        or task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or build_sha == "0" * 40
        or date.fromisoformat(trade_date).isoformat() != trade_date
    ):
        raise RuntimeError("direct analysis publication identity is invalid")
    validate_recommended_run_history_schema(engine)
    with engine.begin() as connection:
        existing = connection.execute(text("""
            SELECT run_uid
            FROM st_recommended_run_history
            WHERE run_uid=:run_uid
            LIMIT 2
        """), {"run_uid": run_uid}).mappings().all()
        if existing:
            raise RuntimeError(
                "direct analysis recommendation history already exists"
            )
        result = connection.execute(text("""
            INSERT INTO st_recommended_run_history
                (run_uid, scheduler_job_id, trade_date, status, min_score,
                 top_n, strict_prev_trade_day, execution_time, started_at,
                 progress_percent, done_count, message, trigger_source,
                 build_sha, publisher_task_type)
            VALUES
                (:run_uid, :run_uid, :trade_date, 'running', :min_score,
                 :top_n, :strict_prev_trade_day, :execution_time,
                 CURRENT_TIMESTAMP, 0, 0, 'scheduled analysis started',
                 'scheduled', :build_sha, :task_type)
        """), {
            "run_uid": run_uid,
            "trade_date": trade_date,
            "min_score": float(min_score),
            "top_n": int(top_n),
            "strict_prev_trade_day": 1 if strict_prev_trade_day else 0,
            "execution_time": (
                str(execution_time or "")[:19].replace("T", " ") or None
            ),
            "build_sha": build_sha,
            "task_type": task_type,
        })
        if int(result.rowcount or 0) != 1:
            raise RuntimeError(
                "direct analysis recommendation history was not prebound"
            )


def _finish_direct_publication_history(
    engine: Engine,
    *,
    run_uid: str,
    success: bool,
    error: str = "",
) -> None:
    """Persist one terminal recommendation audit before scheduler validation."""

    with engine.begin() as connection:
        if success:
            result = connection.execute(text("""
                UPDATE st_recommended_run_history
                SET status='done', finished_at=CURRENT_TIMESTAMP,
                    progress_percent=100, done_count=total,
                    message='scheduled analysis completed', error=NULL
                WHERE run_uid=:run_uid
                  AND scheduler_job_id=:run_uid
                  AND status='running'
                  AND canonical_pool_sha256 IS NOT NULL
                  AND published_at IS NOT NULL
                  AND executable_count>0
            """), {"run_uid": run_uid})
        else:
            result = connection.execute(text("""
                UPDATE st_recommended_run_history
                SET status='error', finished_at=CURRENT_TIMESTAMP,
                    message='scheduled analysis failed', error=:error
                WHERE run_uid=:run_uid
                  AND scheduler_job_id=:run_uid
                  AND status='running'
            """), {
                "run_uid": run_uid,
                "error": str(error or "")[:500],
            })
        if int(result.rowcount or 0) != 1:
            raise RuntimeError(
                "direct analysis recommendation terminal audit differs"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Fast full-market EOD analysis and recommendation batch")
    parser.add_argument("--date", default="", help="Analysis trade date, default latest sm_stock_kline date")
    parser.add_argument("--top-n", type=int, default=80, help="Number of recommendation rows to keep")
    parser.add_argument("--min-score", type=float, default=62.0, help="Minimum AI score for recommendation eligibility")
    parser.add_argument("--strict-prev-trade-day", action="store_true", help="Use only the previous trading day of execution time and fail if data is incomplete")
    parser.add_argument("--execution-time", default="", help="Execution timestamp/date used by --strict-prev-trade-day, default today")
    parser.add_argument("--min-kline-coverage", type=float, default=0.80, help="Minimum K-line coverage ratio for strict runs")
    parser.add_argument("--auto-repair-missing-kline", action="store_true", help="For strict runs, first repair the target day full-market K-line from Guojin QMT when missing")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    engine = create_batch_engine()
    publication_run_uid = str(
        os.environ.get("PROBIGA_SCHEDULER_HISTORY_RUN_UID") or ""
    ).strip().lower()
    publisher_task_type = str(
        os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") or ""
    ).strip()
    publisher_build_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA") or ""
    ).strip().lower()
    publication_identity = any((
        publication_run_uid,
        publisher_task_type,
        publisher_build_sha,
    ))
    history_prebound = False
    try:
        execution_time = (
            args.execution_time.strip()
            or _now_shanghai_naive().replace(microsecond=0).isoformat(sep=" ")
        )
        if args.strict_prev_trade_day:
            resolved_trade_date = previous_trade_date(engine, execution_time)
            if (
                args.date.strip()
                and args.date.strip() != resolved_trade_date
            ):
                raise RuntimeError(
                    "strict direct analysis target differs from previous session"
                )
        else:
            resolved_trade_date = (
                args.date.strip() or latest_trade_date(engine)
            )
        if publication_identity:
            _prebind_direct_publication_history(
                engine,
                run_uid=publication_run_uid,
                task_type=publisher_task_type,
                build_sha=publisher_build_sha,
                trade_date=resolved_trade_date,
                min_score=args.min_score,
                top_n=args.top_n,
                strict_prev_trade_day=args.strict_prev_trade_day,
                execution_time=execution_time,
            )
            history_prebound = True
        stats = run_batch(
            engine=engine,
            trade_date=resolved_trade_date,
            top_n=args.top_n,
            min_score=args.min_score,
            strict_prev_trade_day=args.strict_prev_trade_day,
            execution_time=execution_time,
            min_kline_coverage=args.min_kline_coverage,
            auto_repair_missing_kline=args.auto_repair_missing_kline,
            publication_run_uid=publication_run_uid,
            publisher_task_type=publisher_task_type,
            publisher_build_sha=publisher_build_sha,
        )
        if history_prebound:
            _finish_direct_publication_history(
                engine,
                run_uid=publication_run_uid,
                success=True,
            )
    except Exception as exc:
        if history_prebound:
            try:
                _finish_direct_publication_history(
                    engine,
                    run_uid=publication_run_uid,
                    success=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception as audit_exc:
                raise RuntimeError(
                    "direct analysis failed and terminal audit also failed: "
                    f"original={type(exc).__name__}: {exc}; "
                    f"audit={type(audit_exc).__name__}: {audit_exc}"
                ) from exc
        raise
    payload = {
        "trade_date": stats.trade_date,
        "analysis_count": stats.analysis_count,
        "recommendation_count": stats.recommendation_count,
        "market_mood_score": stats.market_mood_score,
        "flow_date": stats.flow_date,
        "hot_date": stats.hot_date,
        "executable_count": stats.executable_count,
    }
    if stats.publication_receipt is not None:
        # Scheduler publication validation consumes this signed nested receipt.
        # Keep it as one canonical stdout line even when the production task
        # does not pass --json; logging remains on stderr.
        payload["publication_receipt"] = stats.publication_receipt
        print(json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ))
    elif args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"fast analysis done: date={stats.trade_date}, "
            f"analysis={stats.analysis_count}, recommendations={stats.recommendation_count}, "
            f"market_mood={stats.market_mood_score:.1f}, flow_date={stats.flow_date or '-'}, hot_date={stats.hot_date or '-'}"
        )
    engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
