# -*- coding: utf-8 -*-
"""热门数据查询 API + 首页看板"""
import json
import logging
import os
import re
import time as _time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.current_data import get_current_engine, should_use_current_engine
from server.common.kline_data import get_kline_engine, should_use_kline_engine
from server.common.minute_data import (
    get_minute_engine,
    get_stock_minute_prices,
    minute_source_info,
    should_use_capital_flow_engine,
)
from server.common.process_env import build_child_env
from server.common.adata_release import ensure_adata_import_path
from server.common.config import get_api_cache_config, get_settings
from server.common.batch_db import quote_identifier, write_frame
from server.common.sql_reader import read_sql_rows
from server.common.tech_risk import (
    build_tech_risk_signal,
    fetch_tech_risk_signal,
    holding_matches_signal,
    is_tech_exposed,
)
from server.engine.data_loader import StockDataLoader
from server.engine.production_selector import board_limit_trigger_pct

logger = logging.getLogger(__name__)
from server.engine.stock_analysis_engine import StockAnalysisEngine
from server.api.routers.portfolio_math import (
    portfolio_calc_next_position,
    portfolio_cost_profit,
    portfolio_recalc_cost_from_history,
    portfolio_trade_fee,
)

# Chart generation
import sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
try:
    from biz.review.charts import (
        chart_market_heat, chart_sector_bars, chart_index_position, chart_volume_tags,
    )
    _CHARTS_AVAILABLE = True
except Exception as exc:
    logger.debug("review chart helpers are unavailable: %s", exc)
    _CHARTS_AVAILABLE = False

router = APIRouter()

PORTFOLIO_LIVE_FRESH_SECONDS = 90
PORTFOLIO_LIVE_STALE_SECONDS = 300
PORTFOLIO_FLOW_FRESH_SECONDS = 180
NEWS_REQUEST_TIMEOUT_SECONDS = 10.0
PORTFOLIO_SNAPSHOT_LOCK_WAIT_SECONDS = 0.5
PORTFOLIO_SNAPSHOT_ERROR_TTL_SECONDS = 3
PORTFOLIO_FORCE_REQUEST_TTL_SECONDS = 120

LIVE_FUSED_SOURCE_LABEL = "东财人气榜 / 雪球热股 / 新浪热股 / 同花顺热股"


# ── 内存 TTL 缓存（避免重复请求外部 API / 历史数据） ──
import threading as _threading

_cache_lock = _threading.Lock()
_cache_store: OrderedDict[str, tuple[float, object]] = OrderedDict()
_market_sentiment_locks_lock = _threading.Lock()
_market_sentiment_locks: dict[str, object] = {}
_job_lock = _threading.Lock()
_job_running: dict[str, bool] = {
    "recommended_stocks": False,
    "market_refresh": False,
}
_portfolio_qmt_refresh_lock = _threading.Lock()
_portfolio_snapshot_build_lock = _threading.Lock()
_portfolio_snapshot_generation = 0
_portfolio_completed_force_requests: OrderedDict[str, tuple[int, float]] = OrderedDict()
_portfolio_qmt_refresh_thread: _threading.Thread | None = None
_portfolio_qmt_refresh_state: dict[str, object] = {
    "state": "idle",
    "started_at": "",
    "finished_at": "",
    "requested": 0,
    "refreshed": 0,
    "stale": 0,
    "missing": 0,
    "error": "",
}
_fallback_lock = _threading.Lock()
_fallback_events: OrderedDict[str, dict[str, object]] = OrderedDict()
MAX_FALLBACK_EVENTS = 200


def _record_fallback(context: str, exc: BaseException | None = None) -> None:
    """Record a non-fatal fallback path without changing API behavior."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    key = str(context or "unknown")
    error = f"{type(exc).__name__}: {exc}"[:300] if exc is not None else ""
    with _fallback_lock:
        item = _fallback_events.get(key)
        if item is None:
            item = {"context": key, "count": 0, "first_at": now, "last_at": now, "last_error": ""}
            _fallback_events[key] = item
        item["count"] = int(item.get("count") or 0) + 1
        item["last_at"] = now
        item["last_error"] = error
        _fallback_events.move_to_end(key)
        while len(_fallback_events) > MAX_FALLBACK_EVENTS:
            _fallback_events.popitem(last=False)
    if exc is not None:
        logger.debug("hot_data fallback [%s]: %s", key, exc)


def _cache_now() -> float:
    return _time.monotonic()


def _cache_ttl_seconds(value: int) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 60


def _cache_trim_locked() -> None:
    try:
        max_entries = int(get_api_cache_config()["max_entries"])
    except Exception as exc:
        logger.debug("api cache max_entries config invalid, using default: %s", exc)
        max_entries = 512
    max_entries = max(1, max_entries)
    while len(_cache_store) > max_entries:
        _cache_store.popitem(last=False)


def _cache_get(key: str, ttl_seconds: int = 60):
    """获取缓存，未过期返回值，否则返回 None"""
    now = _cache_now()
    ttl_seconds = _cache_ttl_seconds(ttl_seconds)
    with _cache_lock:
        entry = _cache_store.get(key)
        if not entry:
            return None
        if ttl_seconds <= 0 or (now - entry[0]) >= ttl_seconds:
            _cache_store.pop(key, None)
            return None
        _cache_store.move_to_end(key)
        return entry[1]
    return None


def _cache_set(key: str, value):
    """写入缓存"""
    with _cache_lock:
        _cache_store[key] = (_cache_now(), value)
        _cache_store.move_to_end(key)
        _cache_trim_locked()


def _cache_peek(key: str):
    """Return ``(created_at, value)`` without expiring a stale entry."""
    with _cache_lock:
        entry = _cache_store.get(key)
        if not entry:
            return None
        _cache_store.move_to_end(key)
        return entry


def _portfolio_snapshot_cache_publish(
    value: dict,
    *,
    generation: int,
    force_request_id: str = "",
) -> bool:
    """Publish only if no portfolio mutation invalidated this build."""
    with _cache_lock:
        if generation != _portfolio_snapshot_generation:
            return False
        _cache_store["portfolio_snapshot"] = (_cache_now(), value)
        _cache_store.move_to_end("portfolio_snapshot")
        if force_request_id:
            _portfolio_completed_force_requests[force_request_id] = (
                generation,
                _cache_now(),
            )
            _portfolio_completed_force_requests.move_to_end(force_request_id)
            while len(_portfolio_completed_force_requests) > 128:
                _portfolio_completed_force_requests.popitem(last=False)
        _cache_trim_locked()
        return True


def _portfolio_snapshot_entry_is_fresh(entry, ttl_seconds: int) -> bool:
    if entry is None:
        return False
    effective_ttl = _cache_ttl_seconds(ttl_seconds)
    value = entry[1]
    if isinstance(value, dict) and "error" in value:
        effective_ttl = min(effective_ttl, PORTFOLIO_SNAPSHOT_ERROR_TTL_SECONDS)
    return effective_ttl > 0 and (_cache_now() - entry[0]) < effective_ttl


def _portfolio_completed_force_result(force_request_id: str):
    """Return the result of a recently completed idempotent force request."""
    if not force_request_id:
        return None
    with _cache_lock:
        completed = _portfolio_completed_force_requests.get(force_request_id)
        entry = _cache_store.get("portfolio_snapshot")
        if completed is None or entry is None:
            return None
        generation, completed_at = completed
        age_seconds = _cache_now() - completed_at
        value = entry[1]
        if (
            generation != _portfolio_snapshot_generation
            or age_seconds >= PORTFOLIO_FORCE_REQUEST_TTL_SECONDS
            or not isinstance(value, dict)
            or "error" in value
        ):
            _portfolio_completed_force_requests.pop(force_request_id, None)
            return None
        _portfolio_completed_force_requests.move_to_end(force_request_id)
        _cache_store.move_to_end("portfolio_snapshot")
        return value


def _cache_drop(key: str):
    with _cache_lock:
        _cache_store.pop(key, None)


def _cache_drop_prefix(prefix: str):
    with _cache_lock:
        for key in list(_cache_store.keys()):
            if key.startswith(prefix):
                _cache_store.pop(key, None)


def _cache_key_market_sentiment(date_value: str, days: int, top: int) -> str:
    return f"market_sentiment_{date_value}_{days}_{top}"


def _market_sentiment_cache_ttl() -> int:
    return 120 if _is_monitor_trading_time() else 300


def _market_sentiment_key_lock(key: str):
    with _market_sentiment_locks_lock:
        lock = _market_sentiment_locks.get(key)
        if lock is None:
            lock = _threading.Lock()
            _market_sentiment_locks[key] = lock
        return lock


def _json_safe_value(value):
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception as exc:
            _record_fallback('_json_safe_value:193', exc)
    if isinstance(value, float):
        import math
        return value if math.isfinite(value) else None
    return value


def _market_sentiment_result(date_value: str, days: int, top: int) -> dict:
    cache_key = _cache_key_market_sentiment(date_value, days, top)
    ttl = _market_sentiment_cache_ttl()
    cached = _cache_get(cache_key, ttl_seconds=ttl)
    if cached is not None:
        return cached

    with _market_sentiment_key_lock(cache_key):
        cached = _cache_get(cache_key, ttl_seconds=ttl)
        if cached is not None:
            return cached

        from tools.market_sentiment import run_full_analysis

        result = run_full_analysis(lookback_days=days, end_date=date_value, top_n=top, engine=get_engine())
        result = _json_safe_value(result)
        if "error" not in result:
            _cache_set(cache_key, result)
        return result


def _invalidate_recommended_stocks_cache() -> None:
    for prefix in (
        "recommended_stocks_",
        "latest_date_st_recommended_stocks_",
        "latest_date_not_after_st_recommended_stocks_",
    ):
        _cache_drop_prefix(prefix)


def _job_begin(name: str) -> bool:
    with _job_lock:
        if _job_running.get(name):
            return False
        _job_running[name] = True
        return True


def _job_end(name: str) -> None:
    with _job_lock:
        _job_running[name] = False


def _job_is_running(name: str) -> bool:
    with _job_lock:
        return bool(_job_running.get(name))


def _normalize_db_value(value):
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    if should_use_current_engine(sql):
        engine = get_current_engine()
    elif should_use_capital_flow_engine(sql):
        engine = get_minute_engine()
    else:
        engine = get_kline_engine() if should_use_kline_engine(sql) else get_engine()
    return read_sql_rows(engine, sql, params, context="hot_data")


def _table_columns(table_name: str) -> set[str]:
    cache_key = f"table_columns_{table_name}"
    cached = _cache_get(cache_key, ttl_seconds=3600)
    if cached is not None:
        return cached
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
            """), {"table_name": table_name}).fetchall()
        result = {str(r[0]) for r in rows}
        _cache_set(cache_key, result)
        return result
    except Exception:
        return set()


def _select_col(columns: set[str], column: str, default_sql: str, alias: str | None = None) -> str:
    out_alias = alias or column
    if column in columns:
        return f"r.`{column}` AS `{out_alias}`"
    return f"{default_sql} AS `{out_alias}`"


def _analysis_result_select_list(columns: set[str]) -> str:
    return ",\n                   ".join([
        "r.stock_code",
        "r.stock_name",
        "r.analysis_date",
        "r.last_news_time",
        "r.long_term_score",
        "r.fundamental_score",
        "r.growth_score",
        "r.valuation_score",
        "r.risk_score",
        "r.short_term_score",
        "r.capital_score",
        "r.technical_score",
        "r.sentiment_score",
        "r.event_score",
        "r.event_risk_score",
        "r.event_risk_level",
        "r.event_risk_detail",
        "r.recommend_status",
        "r.recommend_reason",
        "r.summary",
        "r.recommendation",
        "r.strengths",
        "r.risks",
        _select_col(columns, "data_quality_score", "NULL"),
        _select_col(columns, "data_quality_flags", "NULL"),
        _select_col(columns, "flow_trade_date", "NULL"),
        _select_col(columns, "hot_trade_date", "NULL"),
        _select_col(columns, "model_version", "''"),
    ])


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    import numpy as np

    if df is None or df.empty:
        return []
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime64"):
            df[c] = df[c].astype(str)
    return [
        {key: _normalize_db_value(val) for key, val in row.items()}
        for row in df.to_dict(orient="records")
    ]


def _fetch_live_east_rank(top: int = 100) -> pd.DataFrame:
    import requests

    today = date.today().isoformat()
    url = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
    headers = {
        "User-Agent": "Mozilla/5.0 ProBigA",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://guba.eastmoney.com",
        "Referer": "https://guba.eastmoney.com/",
    }
    payload = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "pageNo": 1,
        "pageSize": top,
        "date": today.replace("-", ""),
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data") or []
    rows = []
    for item in data[:top]:
        raw_code = str(item.get("sc", ""))
        stock_code = raw_code[2:] if len(raw_code) > 2 else raw_code
        rank_change = item.get("rc", 0) or 0
        rows.append({
            "rank": int(item.get("rk", 0) or 0),
            "stock_code": stock_code,
            "short_name": "",
            "rank_change": rank_change,
            "his_rank": item.get("hisRc", 0),
            "price": None,
            "price_change": None,
            "change_pct": None,
            "hot_value": round((101 - int(item.get("rk", 100) or 100)) / 100 * 100, 1),
            "pop_tag": "排名上升" if rank_change > 0 else ("排名下降" if rank_change < 0 else "排名持平"),
            "concept_tag": None,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    codes = df["stock_code"].astype(str).tolist()
    try:
        placeholders = ",".join([f"'{c}'" for c in codes])
        names = _read_sql(f"SELECT stock_code, short_name FROM si_all_code WHERE stock_code IN ({placeholders})")
        name_map = {str(r["stock_code"]): r.get("short_name") or "" for r in names}
        df["short_name"] = df["stock_code"].map(name_map).fillna("")
    except Exception as exc:
        _record_fallback('_fetch_live_east_rank:392', exc)

    try:
        sina_codes = ",".join([("sh" + c if c.startswith("6") else "sz" + c) for c in codes])
        quote_resp = requests.get(
            f"https://hq.sinajs.cn/list={sina_codes}",
            headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn"},
            timeout=15,
        )
        quote_map = {}
        for line in quote_resp.text.strip().split("\n"):
            if "=" not in line or '""' in line:
                continue
            var_part, val_part = line.split("=", 1)
            code6 = var_part.split("_")[-1][2:]
            fields = val_part.strip('";\r ').split(",")
            if len(fields) >= 4:
                try:
                    prev_close = float(fields[2])
                    current = float(fields[3]) or float(fields[1]) or prev_close
                    price_change = current - prev_close
                    quote_map[code6] = {
                        "price": round(current, 2),
                        "price_change": round(price_change, 2),
                        "change_pct": round(price_change / prev_close * 100, 2) if prev_close else None,
                    }
                except Exception as exc:
                    logger.debug("failed to parse Sina quote line for live rank: %s", exc)
        for col in ("price", "price_change", "change_pct"):
            df[col] = df["stock_code"].map(lambda code: quote_map.get(code, {}).get(col))
    except Exception as exc:
        _record_fallback('_fetch_live_east_rank:423', exc)
    return df


def _fetch_live_ths_rank(top: int = 100) -> pd.DataFrame:
    ensure_adata_import_path(_ROOT)
    from adata.sentiment.hot import Hot

    df = Hot().hot_rank_100_ths()
    if df is None or df.empty:
        return pd.DataFrame()
    return df.head(top).copy()


def _fetch_live_xq_rank(top: int = 100) -> pd.DataFrame:
    from tools.fetch_hot_rank_xq import _fetch_hot_rank_xq, _init_cookie

    _init_cookie()
    df = _fetch_hot_rank_xq()
    if df is None or df.empty:
        return pd.DataFrame()
    return df.head(top).copy()


def _fetch_live_sina_rank(top: int = 100) -> pd.DataFrame:
    import requests

    url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    params = {"page": 1, "num": top, "sort": "attention", "asc": 0, "node": "hs_a"}
    headers = {"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn/"}
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    rows = []
    for i, item in enumerate(resp.json() or [], 1):
        rows.append({
            "rank": i,
            "stock_code": str(item.get("code", "")).zfill(6),
            "short_name": item.get("name", ""),
            "price": float(item.get("trade", 0)) if item.get("trade") else None,
            "price_change": float(item.get("pricechange", 0)) if item.get("pricechange") else None,
            "change_pct": float(item.get("changepercent", 0)) if item.get("changepercent") else None,
            "amount": float(item.get("amount", 0)) if item.get("amount") else None,
            "volume": float(item.get("volume", 0)) if item.get("volume") else None,
            "market_capital": float(item.get("mktcap", 0)) if item.get("mktcap") else None,
            "turnover_ratio": float(item.get("turnoverratio", 0)) if item.get("turnoverratio") else None,
        })
    return pd.DataFrame(rows)


def _live_fused_rank(top: int = 100, *, force_refresh: bool = False) -> dict:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tools.merge_hot_rank import _attach_industry, _filter_hs_a, _fuse_single_day, _load_industry_map

    # 缓存 60 秒，避免频繁切换页面时重复请求外部 API
    cache_key = f"fused_live_{top}"
    cached = _cache_get(cache_key, ttl_seconds=1 if force_refresh else 60)
    if cached is not None:
        return cached

    fetched_at = datetime.now().replace(microsecond=0)
    errors: dict[str, str] = {}
    frames: dict[str, pd.DataFrame] = {}
    fetchers = {
        "east": _fetch_live_east_rank,
        "ths": _fetch_live_ths_rank,
        "xq": _fetch_live_xq_rank,
        "sina": _fetch_live_sina_rank,
    }
    # 并行抓取 4 个外部数据源（原来串行，最坏 60s → 并行约 15s）
    def _fetch_one(source, fetcher):
        try:
            return source, _filter_hs_a(fetcher(top))
        except Exception as exc:
            return source, exc

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch_one, s, f): s for s, f in fetchers.items()}
        for future in as_completed(futures):
            source, result = future.result()
            if isinstance(result, Exception):
                frames[source] = pd.DataFrame()
                errors[source] = str(result)
            else:
                frames[source] = result

    if all(df.empty for df in frames.values()):
        return {
            "date": date.today().isoformat(),
            "data": [],
            "total": 0,
            "live": True,
            "source_label": LIVE_FUSED_SOURCE_LABEL,
            "time": fetched_at.strftime("%H:%M:%S"),
            "fetched_at": fetched_at.isoformat(sep=" "),
            "source_counts": {k: 0 for k in fetchers},
            "errors": errors,
            "error": "实时数据源均未返回数据",
        }

    result_df = _fuse_single_day(frames["east"], frames["ths"], frames["xq"], frames["sina"])
    result_df["fused_rank"] = range(1, len(result_df) + 1)
    result_df["snapshot_date"] = date.today().isoformat()
    result_df["etl_sync_at"] = fetched_at

    try:
        _attach_industry(result_df, _load_industry_map(get_engine()))
    except Exception:
        if "industry_name" not in result_df.columns:
            result_df["industry_name"] = None

    ths_tags = {}
    if not frames["ths"].empty:
        for _, row in frames["ths"].iterrows():
            ths_tags[str(row.get("stock_code", "")).strip()] = {
                "pop_tag": row.get("pop_tag"),
                "concept_tag": row.get("concept_tag"),
            }
    result_df["pop_tag"] = result_df["stock_code"].map(lambda code: ths_tags.get(str(code), {}).get("pop_tag"))
    result_df["concept_tag"] = result_df["stock_code"].map(lambda code: ths_tags.get(str(code), {}).get("concept_tag"))

    top_df = result_df.head(top).copy()
    _result = {
        "date": date.today().isoformat(),
        "data": _df_to_records(top_df),
        "total": len(top_df),
        "live": True,
        "source_label": LIVE_FUSED_SOURCE_LABEL,
        "time": fetched_at.strftime("%H:%M:%S"),
        "fetched_at": fetched_at.isoformat(sep=" "),
        "source_counts": {k: int(len(v)) for k, v in frames.items()},
        "errors": errors,
    }
    _cache_set(cache_key, _result)
    return _result


# ========== API ==========

@router.get("/hot-data/fallback-health")
def fallback_health():
    with _fallback_lock:
        events = [dict(item) for item in _fallback_events.values()]
    total = sum(int(item.get("count") or 0) for item in events)
    events.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("context") or "")))
    return {
        "status": "ok" if total == 0 else "observed",
        "total_fallbacks": total,
        "contexts": events[:100],
    }


@router.get("/hot-data/latest-trade-date")
def latest_trade_date():
    today = date.today().isoformat()
    try:
        expected_trade_date = _market_clock_trade_date_from_calendar(today)
        if expected_trade_date == today:
            rows = _read_sql(
                """
                SELECT COUNT(DISTINCT stock_code) AS cnt
                FROM sm_stock_current
                WHERE DATE(snapshot_at) = :today
                """,
                {"today": today},
            )
            if rows and int(rows[0].get("cnt") or 0) >= 3000:
                return {"latest_date": today, "source": "stock_current"}
    except Exception as exc:
        logger.debug("current snapshot latest trade date lookup failed: %s", exc)
    try:
        expected_trade_date = _market_clock_trade_date_from_calendar(today)
        if expected_trade_date == today:
            rows = _read_sql(
                """
                SELECT DATE(MAX(snapshot_at)) AS d
                FROM sm_rt_quote_snapshot
                """,
            )
            if rows and str(rows[0].get("d") or "")[:10] == today:
                return {"latest_date": today, "source": "rt_quote_snapshot"}
    except Exception as exc:
        logger.debug("archived realtime latest trade date lookup failed: %s", exc)
    try:
        expected_trade_date = _market_clock_trade_date_from_calendar(today)
        if expected_trade_date == today:
            rows = _read_sql(
                """
                SELECT COUNT(*) AS cnt
                FROM st_hot_rank_fused
                WHERE snapshot_date = :today
                """,
                {"today": today},
            )
            if rows and int(rows[0].get("cnt") or 0) >= 20:
                return {"latest_date": today, "source": "hot_rank_fused"}
    except Exception as exc:
        logger.debug("hot rank latest trade date lookup failed: %s", exc)
    try:
        rows = _read_sql(
            """
            SELECT trade_date AS d
            FROM sm_market_overview_daily
            WHERE total >= 3000
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        if rows and rows[0].get("d"):
            return {"latest_date": str(rows[0]["d"])[:10], "source": "market_overview"}
    except Exception as exc:
        logger.debug("market overview latest trade date lookup failed: %s", exc)
    try:
        rows = _read_sql(
            """
            SELECT trade_date AS d
            FROM (
                SELECT trade_date, COUNT(*) AS cnt
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                  AND trade_date BETWEEN DATE_SUB(:today, INTERVAL 45 DAY) AND :today
                GROUP BY trade_date
            ) t
            WHERE cnt >= 3000
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            {"today": today},
        )
        if rows and rows[0].get("d"):
            return {"latest_date": str(rows[0]["d"])[:10], "source": "stock_kline"}
    except Exception as exc:
        logger.debug("kline latest trade date lookup failed: %s", exc)
    try:
        row = _read_sql(
            "SELECT GREATEST("
            "COALESCE((SELECT MAX(snapshot_date) FROM st_hot_rank_fused), '1000-01-01'),"
            "COALESCE((SELECT MAX(snapshot_date) FROM st_hot_concept_ths_daily), '1000-01-01')"
            ") AS d"
        )
        d = row[0]["d"] if row and row[0].get("d") else None
        if d and d > date(1000, 1, 1):
            return {"latest_date": str(d)}
        return {"latest_date": date.today().isoformat()}
    except Exception as exc:
        logger.debug("fallback latest hot-rank date lookup failed: %s", exc)
        return {"latest_date": date.today().isoformat()}


def _market_clock_trade_date_from_calendar(today: str) -> str:
    try:
        row = _read_sql(
            """
            SELECT MAX(trade_date) AS d
            FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date <= :today
            """,
            {"today": today},
        )
        if row and row[0].get("d"):
            return str(row[0]["d"])[:10]
    except Exception as exc:
        logger.debug("trade calendar lookup failed for market clock: %s", exc)
    return today


def _market_clock_latest_data_date(today: str) -> str:
    try:
        expected_trade_date = _market_clock_trade_date_from_calendar(today)
        if expected_trade_date == today:
            rows = _read_sql(
                """
                SELECT GREATEST(
                    COALESCE((
                        SELECT CASE WHEN COUNT(DISTINCT stock_code) >= 3000 THEN DATE(MAX(snapshot_at)) END
                        FROM sm_stock_current
                        WHERE DATE(snapshot_at) = :today
                    ), '1000-01-01'),
                    COALESCE((
                        SELECT CASE WHEN COUNT(*) >= 20 THEN MAX(snapshot_date) END
                        FROM st_hot_rank_fused
                        WHERE snapshot_date = :today
                    ), '1000-01-01'),
                    COALESCE((
                        SELECT CASE WHEN COUNT(*) >= 20 THEN MAX(snapshot_date) END
                        FROM st_hot_concept_ths_daily
                        WHERE snapshot_date = :today
                    ), '1000-01-01')
                ) AS d
                """,
                {"today": today},
            )
            d = str(rows[0].get("d") or "")[:10] if rows else ""
            if d == today:
                return today
    except Exception as exc:
        logger.debug("same-day realtime data-date lookup failed for market clock: %s", exc)
    candidates: list[str] = []
    try:
        row = _read_sql(
            """
            SELECT trade_date AS d
            FROM sm_market_overview_daily
            WHERE total >= 3000
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        if row and row[0].get("d"):
            candidates.append(str(row[0]["d"])[:10])
    except Exception as exc:
        logger.debug("market overview data-date lookup failed for market clock: %s", exc)
    try:
        row = _read_sql(
            """
            SELECT trade_date AS d
            FROM (
                SELECT trade_date, COUNT(*) AS cnt
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                  AND trade_date BETWEEN DATE_SUB(:today, INTERVAL 45 DAY) AND :today
                GROUP BY trade_date
            ) t
            WHERE cnt >= 3000
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            {"today": today},
        )
        if row and row[0].get("d"):
            candidates.append(str(row[0]["d"])[:10])
    except Exception as exc:
        logger.debug("kline data-date lookup failed for market clock: %s", exc)
    if not candidates:
        try:
            row = _read_sql(
                """
                SELECT GREATEST(
                    COALESCE((SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1), '1000-01-01'),
                    COALESCE((SELECT MAX(trade_date) FROM sm_stock_snapshot), '1000-01-01'),
                    COALESCE((SELECT MAX(snapshot_date) FROM st_hot_rank_fused), '1000-01-01'),
                    COALESCE((SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily), '1000-01-01')
                ) AS d
                """
            )
            if row and row[0].get("d") and str(row[0]["d"])[:10] > "1000-01-01":
                candidates.append(str(row[0]["d"])[:10])
        except Exception as exc:
            logger.debug("combined data-date lookup failed for market clock: %s", exc)
    if expected_trade_date == today:
        candidates = [candidate for candidate in candidates if candidate < today]
    else:
        candidates = [candidate for candidate in candidates if candidate <= expected_trade_date]
    return max(candidates) if candidates else (expected_trade_date or today)


def _market_clock_recommendation_trade_date(today: str, expected_trade_date: str, latest_data_date: str) -> str:
    if expected_trade_date != today:
        return expected_trade_date or latest_data_date or today
    try:
        row = _read_sql(
            """
            SELECT MAX(trade_date) AS d
            FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date < :today
            """,
            {"today": today},
        )
        if row and row[0].get("d"):
            return str(row[0]["d"])[:10]
    except Exception as exc:
        logger.debug("recommendation trade-date calendar lookup failed: %s", exc)
    try:
        row = _read_sql(
            """
            SELECT trade_date AS d
            FROM (
                SELECT trade_date, COUNT(*) AS cnt
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                  AND trade_date BETWEEN DATE_SUB(:today, INTERVAL 45 DAY) AND DATE_SUB(:today, INTERVAL 1 DAY)
                GROUP BY trade_date
            ) t
            WHERE cnt >= 3000
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            {"today": today},
        )
        if row and row[0].get("d"):
            return str(row[0]["d"])[:10]
    except Exception as exc:
        logger.debug("recommendation trade-date kline lookup failed: %s", exc)
    return latest_data_date or expected_trade_date or today


def _market_phase(now: datetime, is_trade_day: bool) -> tuple[str, str, bool]:
    hhmm = now.hour * 100 + now.minute
    if not is_trade_day:
        return "closed", "非交易日", False
    if hhmm < 925:
        return "premarket", "盘前", False
    if 925 <= hhmm <= 1135:
        return "intraday", "盘中", True
    if 1135 < hhmm < 1255:
        return "midday_break", "午间休市", False
    if 1255 <= hhmm <= 1505:
        return "intraday", "盘中", True
    return "postmarket", "盘后", False


@router.get("/hot-data/market-clock")
def market_clock():
    now = datetime.now()
    today = now.date().isoformat()
    expected_trade_date = _market_clock_trade_date_from_calendar(today)
    latest_data_date = _market_clock_latest_data_date(today)
    is_trade_day = expected_trade_date == today
    phase, phase_label, is_intraday = _market_phase(now, is_trade_day)
    active_trade_date = today if is_trade_day and phase in {"premarket", "intraday", "midday_break", "postmarket"} else expected_trade_date
    data_date = active_trade_date if is_trade_day and phase in {"intraday", "midday_break", "postmarket"} else (latest_data_date or expected_trade_date)
    if data_date > expected_trade_date and not is_trade_day:
        data_date = expected_trade_date
    recommendation_trade_date = _market_clock_recommendation_trade_date(today, expected_trade_date, latest_data_date)
    return {
        "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "today": today,
        "phase": phase,
        "phase_label": phase_label,
        "is_trade_day": is_trade_day,
        "is_intraday": is_intraday,
        "active_trade_date": active_trade_date,
        "recommendation_trade_date": recommendation_trade_date,
        "expected_trade_date": expected_trade_date,
        "latest_data_date": latest_data_date,
        "ui_trade_date": data_date,
        "data_policy": "盘中使用实时行情和当日交易日；盘后/休市使用最新已落库交易日。",
    }


def _fallback_date(table: str, col: str, requested: str) -> str:
    try:
        quoted_table = quote_identifier(table)
        quoted_col = quote_identifier(col)
        rows = _read_sql(
            f"SELECT {quoted_col} AS d FROM {quoted_table} WHERE {quoted_col} <= :d ORDER BY {quoted_col} DESC LIMIT 1",
            {"d": requested},
        )
        if rows and rows[0].get("d"):
            return str(rows[0]["d"])
    except Exception as exc:
        _record_fallback('_fallback_date:793', exc)
    return requested


@router.get("/hot-data/available-dates")
def available_dates():
    result = {}
    for tbl, col in [
        ("st_hot_rank_fused", "snapshot_date"),
        ("st_hot_rank_multi_day", "stat_date"),
        ("st_hot_concept_ths_daily", "snapshot_date"),
        ("st_hot_rank_ths", "snapshot_date"),
        ("st_hot_pop_rank_east", "snapshot_date"),
        ("st_hot_rank_xq", "snapshot_date"),
        ("st_a_list_daily", "trade_date"),
        ("sm_stock_capital_flow_daily", "trade_date"),
    ]:
        try:
            quoted_tbl = quote_identifier(tbl)
            quoted_col = quote_identifier(col)
            rows = _read_sql(f"SELECT DISTINCT {quoted_col} AS d FROM {quoted_tbl} ORDER BY {quoted_col} DESC")
            result[tbl] = [r["d"] for r in rows]
        except Exception:
            result[tbl] = []
    return result


@router.get("/hot-data/fused")
def fused(snapshot_date: str = Query(default_factory=lambda: date.today().isoformat()), top: int = 100):
    try:
        rows = _read_sql("""
            SELECT f.*, t.pop_tag, t.concept_tag
            FROM st_hot_rank_fused f
            LEFT JOIN st_hot_rank_ths t ON t.stock_code = f.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = f.snapshot_date
            WHERE f.snapshot_date = :d
            ORDER BY f.fused_rank LIMIT :n
        """, {"d": snapshot_date, "n": top})
        if not rows:
            fb = _fallback_date("st_hot_rank_fused", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("""
                    SELECT f.*, t.pop_tag, t.concept_tag
                    FROM st_hot_rank_fused f
                    LEFT JOIN st_hot_rank_ths t ON t.stock_code = f.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = f.snapshot_date
                    WHERE f.snapshot_date = :d
                    ORDER BY f.fused_rank LIMIT :n
                """, {"d": fb, "n": top})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": snapshot_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": snapshot_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/fused-live")
def fused_live(
    top: int = Query(default=100, ge=1, le=200),
    fresh: bool = Query(default=False),
):
    """盘中实时融合榜：直接抓取东财/同花顺/雪球/新浪热股并即时融合，不落库。"""
    try:
        return _live_fused_rank(top, force_refresh=bool(fresh))
    except Exception as e:
        return {
            "date": date.today().isoformat(),
            "data": [],
            "total": 0,
            "live": True,
            "source_label": LIVE_FUSED_SOURCE_LABEL,
            "time": datetime.now().strftime("%H:%M:%S"),
            "error": str(e),
        }


@router.get("/hot-data/multi-day")
def multi_day(stat_date: str = Query(default_factory=lambda: date.today().isoformat()), days: int = 3, top: int = 100):
    try:
        rows = _read_sql("""
            SELECT m.*, t.pop_tag, t.concept_tag
            FROM st_hot_rank_multi_day m
            LEFT JOIN st_hot_rank_ths t ON t.stock_code = m.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = m.stat_date
            WHERE m.stat_date = :d AND m.stat_days = :n
            ORDER BY m.fused_rank LIMIT :t
        """, {"d": stat_date, "n": days, "t": top})
        if not rows:
            fb = _fallback_date("st_hot_rank_multi_day", "stat_date", stat_date)
            if fb != stat_date:
                rows = _read_sql("""
                    SELECT m.*, t.pop_tag, t.concept_tag
                    FROM st_hot_rank_multi_day m
                    LEFT JOIN st_hot_rank_ths t ON t.stock_code = m.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = m.stat_date
                    WHERE m.stat_date = :d AND m.stat_days = :n
                    ORDER BY m.fused_rank LIMIT :t
                """, {"d": fb, "n": days, "t": top})
                return {"date": fb, "days": days, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": stat_date, "days": days, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": stat_date, "days": days, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/concept-ths")
def concept_ths(snapshot_date: str = Query(default_factory=lambda: date.today().isoformat())):
    try:
        rows = _read_sql("SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d ORDER BY plate_type, `rank`", {"d": snapshot_date})
        if not rows:
            fb = _fallback_date("st_hot_concept_ths_daily", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d ORDER BY plate_type, `rank`", {"d": fb})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": snapshot_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": snapshot_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/concept-ths-live")
def concept_ths_live():
    """同花顺热门概念/行业TOP20（盘中实时，直接调同花顺API）"""
    # 缓存 60 秒
    cached = _cache_get("concept_ths_live", ttl_seconds=60)
    if cached is not None:
        return cached
    try:
        from concurrent.futures import ThreadPoolExecutor
        from datetime import datetime as _dt
        ensure_adata_import_path(_ROOT)
        from adata.sentiment.hot import Hot
        hot = Hot()
        all_data = []
        ts = _dt.now().strftime("%H:%M")

        # 并行获取概念(1)和行业(2)两种板块类型
        def _fetch_plate(pt):
            df = hot.hot_concept_20_ths(plate_type=pt)
            rows = []
            if df is not None and not df.empty:
                for _, r in df.iterrows():
                    rows.append({
                        "plate_type": pt,
                        "rank": int(r.get("rank", 0)),
                        "concept_code": str(r.get("concept_code", "")),
                        "concept_name": str(r.get("concept_name", "")),
                        "change_pct": float(r["change_pct"]) if r.get("change_pct") else None,
                        "hot_value": float(r["hot_value"]) if r.get("hot_value") else None,
                        "hot_tag": str(r.get("hot_tag", "")) if r.get("hot_tag") else None,
                        "live_time": ts,
                    })
            return rows

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(_fetch_plate, pt) for pt in (1, 2)]
            for f in futures:
                all_data.extend(f.result())

        result = {"data": all_data, "total": len(all_data), "live": True, "time": ts}
        _cache_set("concept_ths_live", result)
        return result
    except Exception as e:
        return {"data": [], "total": 0, "live": True, "error": str(e)}


@router.get("/hot-data/rank-ths")
def rank_ths(snapshot_date: str = Query(default_factory=lambda: date.today().isoformat()), top: int = 100):
    try:
        rows = _read_sql("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY `rank` LIMIT :n", {"d": snapshot_date, "n": top})
        if not rows:
            fb = _fallback_date("st_hot_rank_ths", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY `rank` LIMIT :n", {"d": fb, "n": top})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": snapshot_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": snapshot_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/pop-rank-east")
def pop_rank_east(snapshot_date: str = Query(default_factory=lambda: date.today().isoformat()), top: int = 100):
    try:
        rows = _read_sql("""
            SELECT e.*,
                    COALESCE(NULLIF(t.pop_tag, ''), NULLIF(e.pop_tag, ''),
                              CASE WHEN e.rank_change > 0 THEN CONCAT('排名上升', e.rank_change)
                                   WHEN e.rank_change < 0 THEN CONCAT('排名下降', ABS(e.rank_change))
                                   ELSE '排名持平' END) AS pop_tag_final
             FROM st_hot_pop_rank_east e
             LEFT JOIN st_hot_rank_ths t ON t.stock_code = e.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = e.snapshot_date
             WHERE e.snapshot_date = :d
            ORDER BY e.`rank` LIMIT :n
        """, {"d": snapshot_date, "n": top})
        for r in rows:
            r["pop_tag"] = r.pop("pop_tag_final")
        if not rows:
            fb = _fallback_date("st_hot_pop_rank_east", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("""
                    SELECT e.*,
                           COALESCE(NULLIF(t.pop_tag, ''), NULLIF(e.pop_tag, ''),
                                      CASE WHEN e.rank_change > 0 THEN CONCAT('排名上升', e.rank_change)
                                           WHEN e.rank_change < 0 THEN CONCAT('排名下降', ABS(e.rank_change))
                                           ELSE '排名持平' END) AS pop_tag_final
                     FROM st_hot_pop_rank_east e
                     LEFT JOIN st_hot_rank_ths t ON t.stock_code = e.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = e.snapshot_date
                     WHERE e.snapshot_date = :d
                    ORDER BY e.`rank` LIMIT :n
                """, {"d": fb, "n": top})
                for r in rows:
                    r["pop_tag"] = r.pop("pop_tag_final")
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": snapshot_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": snapshot_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/rank-sina")
def rank_sina(top: int = Query(default=100, ge=1, le=200)):
    """新浪热股榜 - 按关注热度实时排行（不落库）"""
    try:
        import httpx
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        params = {"page": 1, "num": top, "sort": "attention", "asc": 0, "node": "hs_a"}
        with httpx.Client(timeout=10, headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn/"}) as c:
            r = c.get(url, params=params, timeout=10)
            data = r.json()
        items = []
        for i, item in enumerate(data, 1):
            items.append({
                "rank": i,
                "stock_code": item.get("code", ""),
                "short_name": item.get("name", ""),
                "price": float(item.get("trade", 0)),
                "price_change": float(item.get("pricechange", 0)),
                "change_pct": float(item.get("changepercent", 0)),
                "amount": float(item.get("amount", 0)),
                "volume": float(item.get("volume", 0)),
                "market_capital": float(item.get("mktcap", 0)),
                "turnover_ratio": float(item.get("turnoverratio", 0)),
            })
        return {"date": date.today().isoformat(), "data": items, "total": len(items)}
    except Exception as e:
        return {"date": date.today().isoformat(), "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/rank-xq")
def rank_xq(snapshot_date: str = Query(default_factory=lambda: date.today().isoformat()), top: int = 100):
    try:
        rows = _read_sql("""
            SELECT x.*, t.pop_tag AS ths_pop_tag, t.concept_tag AS ths_concept_tag
            FROM st_hot_rank_xq x
            LEFT JOIN st_hot_rank_ths t ON t.stock_code = x.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = x.snapshot_date
            WHERE x.snapshot_date = :d
            ORDER BY x.`rank` LIMIT :n
        """, {"d": snapshot_date, "n": top})
        for r in rows:
            r["pop_tag"] = r.pop("ths_pop_tag")
            r["concept_tag"] = r.pop("ths_concept_tag")
        if not rows:
            fb = _fallback_date("st_hot_rank_xq", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("""
                    SELECT x.*, t.pop_tag AS ths_pop_tag, t.concept_tag AS ths_concept_tag
                    FROM st_hot_rank_xq x
                    LEFT JOIN st_hot_rank_ths t ON t.stock_code = x.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date = x.snapshot_date
                    WHERE x.snapshot_date = :d
                    ORDER BY x.`rank` LIMIT :n
                """, {"d": fb, "n": top})
                for r in rows:
                    r["pop_tag"] = r.pop("ths_pop_tag")
                    r["concept_tag"] = r.pop("ths_concept_tag")
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": snapshot_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": snapshot_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/a-list-daily")
def a_list_daily(trade_date: str = Query(default_factory=lambda: date.today().isoformat())):
    try:
        rows = _read_sql("SELECT id, trade_date, short_name, stock_code, close, change_cpt, turnover_ratio, a_net_amount, a_buy_amount, a_sell_amount, a_amount, amount, net_amount_rate, a_amount_rate, reason FROM st_a_list_daily WHERE trade_date = :d ORDER BY stock_code", {"d": trade_date})
        if not rows:
            fb = _fallback_date("st_a_list_daily", "trade_date", trade_date)
            if fb != trade_date:
                rows = _read_sql("SELECT id, trade_date, short_name, stock_code, close, change_cpt, turnover_ratio, a_net_amount, a_buy_amount, a_sell_amount, a_amount, amount, net_amount_rate, a_amount_rate, reason FROM st_a_list_daily WHERE trade_date = :d ORDER BY stock_code", {"d": fb})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": trade_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": trade_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/a-list-info")
def a_list_info(trade_date: str = Query(), stock_code: str = Query()):
    try:
        summary_rows = _read_sql("""
            SELECT trade_date, stock_code, short_name, close, change_cpt, turnover_ratio,
                   a_net_amount, a_buy_amount, a_sell_amount, a_amount, amount,
                   net_amount_rate, a_amount_rate, reason
            FROM st_a_list_daily
            WHERE trade_date = :d AND stock_code = :c
            LIMIT 1
        """, {"d": trade_date, "c": stock_code})
        rows = _read_sql("SELECT id, trade_date, stock_code, operate_name, a_net_amount, a_buy_amount, a_sell_amount, a_buy_amount_rate, a_sell_amount_rate, reason FROM st_a_list_info WHERE trade_date = :d AND stock_code = :c ORDER BY a_net_amount DESC", {"d": trade_date, "c": stock_code})
        summary = summary_rows[0] if summary_rows else {}
        if not summary:
            buy_sum = sum(float(r.get("a_buy_amount") or 0) for r in rows)
            sell_sum = sum(float(r.get("a_sell_amount") or 0) for r in rows)
            summary = {
                "trade_date": trade_date,
                "stock_code": stock_code,
                "a_buy_amount": buy_sum,
                "a_sell_amount": sell_sum,
                "a_net_amount": buy_sum - sell_sum,
                "reason": rows[0].get("reason") if rows else "",
            }
        return {"date": trade_date, "stock_code": stock_code, "summary": summary, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": trade_date, "stock_code": stock_code, "data": [], "total": 0, "error": str(e)}


def _capital_flow_to_float(v):
    if v in (None, "", "-", "--"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fetch_live_capital_flow_rank(sort: str, top: int, stock_code: str = "") -> list[dict]:
    """Fetch Eastmoney real-time A-share capital flow ranking."""
    import requests

    code_kw = stock_code.strip()
    page_size = 6000 if code_kw or top <= 0 else min(max(top, 1), 6000)
    resp = requests.get(
        "https://push2delay.eastmoney.com/api/qt/clist/get",
        params={
            "pn": 1,
            "pz": page_size,
            "po": 1 if sort != "asc" else 0,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f62",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f2,f3,f62,f66,f72,f78,f84,f184",
        },
        headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://data.eastmoney.com/"},
        timeout=15,
    )
    resp.raise_for_status()
    items = ((resp.json().get("data") or {}).get("diff") or [])
    rows = []
    today = date.today().isoformat()
    for item in items:
        code = str(item.get("f12") or "").strip().zfill(6)
        if not code:
            continue
        if code_kw and code_kw not in code:
            continue
        row = {
            "stock_code": code,
            "short_name": item.get("f14") or "",
            "trade_date": today,
            "price": _capital_flow_to_float(item.get("f2")),
            "change_pct": _capital_flow_to_float(item.get("f3")),
            "main_net_inflow": _capital_flow_to_float(item.get("f62")),
            "max_net_inflow": _capital_flow_to_float(item.get("f66")),
            "lg_net_inflow": _capital_flow_to_float(item.get("f72")),
            "mid_net_inflow": _capital_flow_to_float(item.get("f78")),
            "sm_net_inflow": _capital_flow_to_float(item.get("f84")),
            "main_net_rate": _capital_flow_to_float(item.get("f184")),
            "source": "eastmoney_live",
        }
        rows.append(row)
    rows.sort(key=lambda r: r.get("main_net_inflow") or 0, reverse=(sort != "asc"))
    if top > 0:
        rows = rows[:top]
    return rows


def _capital_flow_daily_rows(trade_date: str, sort: str, top: int, stock_code: str = "") -> tuple[list[dict], str, bool]:
    order = "DESC" if sort == "desc" else "ASC"
    params = {"d": trade_date}
    where = "f.trade_date = :d"
    if stock_code.strip():
        where += " AND f.stock_code LIKE :c"
        params["c"] = f"%{stock_code.strip()}%"
    # The flow table contains only the money-flow columns.  Reuse the
    # same-day market snapshot (and its daily K-line fallback) so the capital
    # page can show the quote fields instead of rendering price/change as '-'.
    sql = f"""
        SELECT f.id, f.stock_code,
               COALESCE(snap.short_name, codes.short_name, '') AS short_name,
               f.trade_date,
               COALESCE(snap.price, kline.close) AS price,
               COALESCE(snap.change_pct, kline.change_pct) AS change_pct,
               COALESCE(snap.amount, kline.amount) AS amount,
               f.main_net_inflow, f.max_net_inflow, f.lg_net_inflow,
               f.mid_net_inflow, f.sm_net_inflow, f.data_source
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code codes ON f.stock_code = codes.stock_code
        LEFT JOIN sm_stock_snapshot snap
               ON snap.stock_code = f.stock_code AND snap.trade_date = f.trade_date
        LEFT JOIN sm_stock_kline kline
               ON kline.stock_code = f.stock_code
              AND kline.trade_date = f.trade_date
              AND kline.k_type = 1
              AND kline.adjust_type = 0
        WHERE {where}
        ORDER BY f.main_net_inflow {order}
    """
    if top > 0:
        sql += f" LIMIT {top}"
    rows = _read_sql(sql, params)
    if not rows and not stock_code.strip():
        fb = _fallback_date("sm_stock_capital_flow_daily", "trade_date", trade_date)
        if fb != trade_date:
            params["d"] = fb
            rows = _read_sql(sql, params)
            return rows, fb, True
    return rows, trade_date, False


def _capital_flow_daily_time(trade_date: str) -> str:
    try:
        rows = _read_sql(
            "SELECT MAX(etl_sync_at) AS data_time FROM sm_stock_capital_flow_daily WHERE trade_date = :d",
            {"d": trade_date},
        )
        if rows and rows[0].get("data_time"):
            return str(rows[0]["data_time"])[:19]
    except Exception as exc:
        _record_fallback('_capital_flow_daily_time:1201', exc)
    return f"{trade_date} 15:00:00"


@router.get("/hot-data/capital-flow")
def capital_flow(
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
    sort: str = Query(default="desc"),
    top: int = Query(default=100),
    stock_code: str = Query(default=""),
):
    try:
        mode = _portfolio_market_mode()
        live_error = ""
        if mode == "intraday":
            try:
                rows = _fetch_live_capital_flow_rank(sort, top, stock_code)
                if rows:
                    snapshot_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    return {
                        "date": date.today().isoformat(),
                        "data": rows,
                        "total": len(rows),
                        "sort": sort,
                        "top": top,
                        "mode": "intraday",
                        "mode_label": "盘中实时",
                        "source": "eastmoney_live",
                        "snapshot_at": snapshot_at,
                        "data_time": snapshot_at,
                    }
            except Exception as e:
                live_error = str(e)[:200]

        target_date = _portfolio_close_trade_date() if mode != "intraday" else trade_date
        rows, used_date, fallback = _capital_flow_daily_rows(target_date, sort, top, stock_code)
        label = "盘后日终" if mode != "intraday" else "实时失败，已回落日级"
        result = {
            "date": used_date,
            "fallback": fallback,
            "data": rows,
            "total": len(rows),
            "sort": sort,
            "top": top,
            "mode": "post_close" if mode != "intraday" else "intraday_fallback",
            "mode_label": label,
            "source": "daily_db",
            "data_time": _capital_flow_daily_time(used_date),
        }
        if live_error:
            result["live_error"] = live_error
        return result
    except Exception as e:
        return {"date": trade_date, "data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/capital-flow-realtime")
def capital_flow_realtime(stock_code: str = Query()):
    try:
        code = stock_code.strip().zfill(6)
        qmt_refresh = {"status": "skipped"}
        qmt_rows = []
        if _is_monitor_trading_time():
            qmt_refresh = _portfolio_refresh_qmt_min_flow([code], force=True)
        try:
            qmt_rows = _read_sql(
                """
                SELECT stock_code, trade_time, main_net_inflow, max_net_inflow,
                       lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
                FROM sm_stock_capital_flow_min
                WHERE stock_code = :code
                  AND trade_time >= CONCAT(CURDATE(), ' 00:00:00')
                ORDER BY trade_time
                """,
                {"code": code},
            )
        except Exception:
            qmt_rows = []
        if qmt_rows:
            records = []
            for row in qmt_rows:
                item = dict(row)
                item["trade_time"] = str(item.get("trade_time") or "")[:19]
                records.append(item)
            latest = dict(records[-1])
            age_seconds = _portfolio_time_age_seconds(latest.get("trade_time"))
            latest["flow_age_seconds"] = age_seconds
            latest["flow_status"] = (
                "fresh"
                if age_seconds is not None and age_seconds <= PORTFOLIO_FLOW_FRESH_SECONDS
                else "stale"
            )
            return {
                "stock_code": code,
                "data": records,
                "latest": latest,
                "total": len(records),
                "source": "qmt_min_flow",
                "refresh": qmt_refresh,
                "flow_status": latest["flow_status"],
                "flow_age_seconds": age_seconds,
            }

        from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow
        cf = StockCapitalFlow()
        df = cf.get_capital_flow_min(stock_code=code)
        if df is None or df.empty:
            return {"stock_code": code, "data": [], "latest": None, "total": 0, "source": "adata", "refresh": qmt_refresh}
        import numpy as np
        df = df.replace({np.nan: None, pd.NaT: None})
        latest = df.iloc[-1].to_dict()
        latest["trade_time"] = str(latest["trade_time"])
        latest["flow_age_seconds"] = _portfolio_time_age_seconds(latest.get("trade_time"))
        latest["flow_status"] = "fresh" if latest["flow_age_seconds"] is not None and latest["flow_age_seconds"] <= PORTFOLIO_FLOW_FRESH_SECONDS else "stale"
        records = df.to_dict(orient="records")
        for r in records:
            r["trade_time"] = str(r["trade_time"])
        return {"stock_code": code, "data": records, "latest": latest, "total": len(records), "source": "adata", "refresh": qmt_refresh, "flow_status": latest["flow_status"], "flow_age_seconds": latest["flow_age_seconds"]}
    except Exception as e:
        return {"stock_code": stock_code, "data": [], "latest": None, "total": 0, "error": str(e)}


@router.get("/hot-data/concept-stocks")
def concept_stocks(concept_code: str = Query(), trade_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """获取某个概念/行业下的个股及当日表现"""
    try:
        kline_join = """
            LEFT JOIN sm_stock_current s ON c.stock_code = s.stock_code
            LEFT JOIN (
                SELECT k1.stock_code, k1.close, k1.change_pct,
                       COALESCE(
                           k1.pre_close,
                           (SELECT k0.close FROM sm_stock_kline k0
                            WHERE k0.stock_code = k1.stock_code AND k0.trade_date < k1.trade_date
                            ORDER BY k0.trade_date DESC LIMIT 1)
                       ) AS pre_close
                FROM sm_stock_kline k1
                INNER JOIN (
                    SELECT stock_code, MAX(trade_date) AS max_date
                    FROM sm_stock_kline GROUP BY stock_code
                ) k2 ON k1.stock_code = k2.stock_code AND k1.trade_date = k2.max_date
            ) k ON c.stock_code = k.stock_code
        """
        change_expr = """
            CASE WHEN sub.chg_pct IS NOT NULL THEN sub.chg_pct
                 WHEN sub.kline_close IS NOT NULL AND sub.prev_close IS NOT NULL AND sub.prev_close > 0
                 THEN ROUND((sub.kline_close - sub.prev_close) / sub.prev_close * 100, 2)
                 ELSE NULL END AS change_pct
        """

        def _resolve_ths_keys(code: str) -> list[str]:
            keys = [code]
            try:
                rows = _read_sql(
                    "SELECT index_code FROM si_concept_code_ths WHERE index_code = :c",
                    {"c": code}
                )
                for r in rows:
                    v = str(r.get("index_code", "")).strip()
                    if v and v not in keys:
                        keys.append(v)
            except Exception as exc:
                _record_fallback('_resolve_ths_keys:1363', exc)
            return keys

        def _resolve_east_codes(code: str) -> list[str]:
            codes = [code]
            if code.startswith("BK"):
                return codes
            try:
                rows = _read_sql(
                    "SELECT concept_name FROM st_hot_concept_ths_daily WHERE concept_code = :c LIMIT 1",
                    {"c": code}
                )
                if rows:
                    name = str(rows[0].get("concept_name", "")).strip()
                    if name:
                        east_rows = _read_sql(
                            "SELECT concept_code FROM si_concept_code_east "
                            "WHERE name LIKE :n ORDER BY LENGTH(name) ASC LIMIT 3",
                            {"n": f"%{name}%"}
                        )
                        for r in east_rows:
                            v = str(r.get("concept_code", "")).strip()
                            if v and v not in codes:
                                codes.append(v)
            except Exception as exc:
                _record_fallback('_resolve_east_codes:1388', exc)
            return codes

        ths_keys = _resolve_ths_keys(concept_code)
        placeholders = " OR ".join([f"c.query_key = :k{i}" for i in range(len(ths_keys))])
        params = {f"k{i}": v for i, v in enumerate(ths_keys)}
        ths_sql = f"""
            SELECT sub.stock_code, sub.short_name, sub.price, {change_expr}
            FROM (
                SELECT DISTINCT c.stock_code, c.short_name,
                       COALESCE(s.price, k.close) AS price,
                       COALESCE(s.change_pct, k.change_pct) AS chg_pct,
                       k.close AS kline_close,
                       k.pre_close AS prev_close
                FROM si_concept_constituent_ths c
                {kline_join}
                WHERE {placeholders}
            ) sub
            ORDER BY change_pct IS NOT NULL DESC, change_pct DESC
        """
        rows = _read_sql(ths_sql, params)

        # 如果没查到且可能是行业代码，尝试用 industry_code 查询
        if not rows:
            ind_sql = f"""
                SELECT sub.stock_code, sub.short_name, sub.price, {change_expr}
                FROM (
                    SELECT DISTINCT c.stock_code, c.short_name,
                           COALESCE(s.price, k.close) AS price,
                           COALESCE(s.change_pct, k.change_pct) AS chg_pct,
                           k.close AS kline_close,
                           k.pre_close AS prev_close
                    FROM si_concept_constituent_ths c
                    {kline_join}
                    WHERE c.query_type = 'industry_code' AND c.query_key = :ind_code
                ) sub
                ORDER BY change_pct IS NOT NULL DESC, change_pct DESC
            """
            rows = _read_sql(ind_sql, {"ind_code": concept_code})

        if not rows:
            east_codes = _resolve_east_codes(concept_code)
            e_placeholders = " OR ".join([f"c.concept_code = :e{i}" for i in range(len(east_codes))])
            e_params = {f"e{i}": v for i, v in enumerate(east_codes)}
            east_sql = f"""
                SELECT sub.stock_code, sub.short_name, sub.price, {change_expr}
                FROM (
                    SELECT DISTINCT c.stock_code, c.short_name,
                           COALESCE(s.price, k.close) AS price,
                           COALESCE(s.change_pct, k.change_pct) AS chg_pct,
                           k.close AS kline_close,
                           k.pre_close AS prev_close
                    FROM si_concept_constituent_east c
                    {kline_join}
                    WHERE {e_placeholders}
                ) sub
                ORDER BY change_pct IS NOT NULL DESC, change_pct DESC
            """
            rows = _read_sql(east_sql, e_params)

        return {"concept_code": concept_code, "date": trade_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"concept_code": concept_code, "data": [], "total": 0, "error": str(e)}


def _safe_float(value):
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _aggregate_concept_multi_day_rows(rows: list[dict], days: int) -> list[dict]:
    grouped: dict[tuple[str, str, int], dict] = {}
    for row in rows:
        concept_code = str(row.get("concept_code") or "").strip()
        if not concept_code:
            continue
        concept_name = str(row.get("concept_name") or "").strip()
        plate_type = int(row.get("plate_type") or 0)
        snapshot_date = str(row.get("snapshot_date") or "")[:10]
        key = (concept_code, concept_name, plate_type)
        item = grouped.setdefault(key, {
            "concept_code": concept_code,
            "concept_name": concept_name,
            "plate_type": plate_type,
            "appear_dates": set(),
            "rank_sum": 0.0,
            "rank_count": 0,
            "best_rank": None,
            "change_sum": 0.0,
            "change_count": 0,
            "hot_sum": 0.0,
            "hot_count": 0,
            "last_snapshot_date": "",
            "last_rank": None,
            "last_change_pct": None,
            "last_hot_value": None,
        })
        if snapshot_date:
            item["appear_dates"].add(snapshot_date)

        rank_val = _safe_float(row.get("rank"))
        if rank_val is not None:
            item["rank_sum"] += rank_val
            item["rank_count"] += 1
            item["best_rank"] = rank_val if item["best_rank"] is None else min(item["best_rank"], rank_val)

        change_val = _safe_float(row.get("change_pct"))
        if change_val is not None:
            item["change_sum"] += change_val
            item["change_count"] += 1

        hot_val = _safe_float(row.get("hot_value"))
        if hot_val is not None:
            item["hot_sum"] += hot_val
            item["hot_count"] += 1

        if snapshot_date >= item["last_snapshot_date"]:
            item["last_snapshot_date"] = snapshot_date
            item["last_rank"] = rank_val
            item["last_change_pct"] = change_val
            item["last_hot_value"] = hot_val

    result = []
    for item in grouped.values():
        appear_days = len(item["appear_dates"])
        avg_rank = (item["rank_sum"] / item["rank_count"]) if item["rank_count"] else None
        avg_change_pct = (item["change_sum"] / item["change_count"]) if item["change_count"] else None
        avg_hot_value = (item["hot_sum"] / item["hot_count"]) if item["hot_count"] else None
        result.append({
            "concept_code": item["concept_code"],
            "concept_name": item["concept_name"],
            "plate_type": item["plate_type"],
            "appear_days": appear_days,
            "appear_pct": round(appear_days / max(days, 1) * 100, 1),
            "avg_rank": round(avg_rank, 2) if avg_rank is not None else None,
            "best_rank": round(item["best_rank"], 2) if item["best_rank"] is not None else None,
            "avg_change_pct": round(avg_change_pct, 2) if avg_change_pct is not None else None,
            "avg_hot_value": round(avg_hot_value, 2) if avg_hot_value is not None else None,
            "last_rank": round(item["last_rank"], 2) if item["last_rank"] is not None else None,
            "last_change_pct": round(item["last_change_pct"], 2) if item["last_change_pct"] is not None else None,
            "last_hot_value": round(item["last_hot_value"], 2) if item["last_hot_value"] is not None else None,
        })

    result.sort(key=lambda row: (-int(row["appear_days"]), float(row["avg_rank"]) if row["avg_rank"] is not None else 1e15))
    return result


@router.get("/hot-data/concept-multi-day")
def concept_multi_day(stat_date: str = Query(default_factory=lambda: date.today().isoformat()), days: int = 3, plate_type: int = 0):
    """近N天热门概念/行业聚合"""
    try:
        is_fallback = False
        if days <= 1:
            rows = _read_sql(
                "SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d" +
                (" AND plate_type = :p" if plate_type > 0 else "") + " ORDER BY plate_type, `rank`",
                {"d": stat_date, "p": plate_type} if plate_type > 0 else {"d": stat_date}
            )
            if not rows:
                fb = _fallback_date("st_hot_concept_ths_daily", "snapshot_date", stat_date)
                if fb != stat_date:
                    rows = _read_sql(
                        "SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d" +
                        (" AND plate_type = :p" if plate_type > 0 else "") + " ORDER BY plate_type, `rank`",
                        {"d": fb, "p": plate_type} if plate_type > 0 else {"d": fb}
                    )
                    stat_date = fb
                    is_fallback = True
            resp = {"date": stat_date, "days": 1, "data": rows, "total": len(rows)}
            if is_fallback:
                resp["fallback"] = True
            return resp

        from datetime import timedelta, datetime
        dt = datetime.strptime(stat_date, "%Y-%m-%d")
        dates = [(dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

        all_rows = []
        for d in dates:
            params = {"d": d}
            plate_sql = ""
            if plate_type > 0:
                plate_sql = " AND plate_type = :p"
                params["p"] = plate_type
            batch = _read_sql(
                f"SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name, change_pct, hot_value, hot_tag FROM st_hot_concept_ths_daily WHERE snapshot_date = :d{plate_sql}",
                params
            )
            all_rows.extend(batch)

        if not all_rows:
            fb = _fallback_date("st_hot_concept_ths_daily", "snapshot_date", stat_date)
            if fb != stat_date:
                from datetime import timedelta as _td, datetime as _dt
                fb_dt = _dt.strptime(fb, "%Y-%m-%d")
                fb_dates = [(fb_dt - _td(days=i)).strftime("%Y-%m-%d") for i in range(days)]
                for d in fb_dates:
                    params = {"d": d}
                    plate_sql = ""
                    if plate_type > 0:
                        plate_sql = " AND plate_type = :p"
                        params["p"] = plate_type
                    batch = _read_sql(
                        f"SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name, change_pct, hot_value, hot_tag FROM st_hot_concept_ths_daily WHERE snapshot_date = :d{plate_sql}",
                        params
                    )
                    all_rows.extend(batch)
                if all_rows:
                    stat_date = fb
                    is_fallback = True
            if not all_rows:
                return {"date": stat_date, "days": days, "data": [], "total": 0}

        result = _aggregate_concept_multi_day_rows(all_rows, days)

        resp = {"date": stat_date, "days": days, "data": result, "total": len(result)}
        if is_fallback:
            resp["fallback"] = True
        return resp
    except Exception as e:
        return {"date": stat_date, "days": days, "data": [], "total": 0, "error": str(e)}


def _fetch_cls_news(client, pages=2):
    import re as _re
    items = []
    last_time = int(_time.time())
    for _ in range(pages):
        r = client.get(
            "https://www.cls.cn/api/cache",
            params={"rn": 50, "lastTime": last_time, "name": "telegraph"},
            timeout=NEWS_REQUEST_TIMEOUT_SECONDS,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("errno") not in (None, 0, "0"):
            raise RuntimeError(f"CLS telegraph API error: {payload.get('errno')}")
        roll_data = (payload.get("data") or {}).get("roll_data") or []
        if not roll_data:
            break
        for it in roll_data:
            stocks = it.get("stock_list") or []
            stock_info = [{"name": s.get("stock_name", ""), "code": s.get("stock_code", "")} for s in stocks[:10] if s.get("stock_name")]
            content = it.get("content") or it.get("brief") or ""
            content_clean = _re.sub(r"<[^>]+>", "", content)
            ts = it.get("ctime") or it.get("modified_time")
            dt_obj = None
            time_str = ""
            if ts:
                from datetime import datetime as _dt
                try:
                    dt_obj = _dt.fromtimestamp(int(ts))
                    time_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    time_str = str(ts)
            subjects = it.get("subjects") or []
            items.append({
                "source": "cls",
                "source_id": str(it.get("id") or ""),
                "title": it.get("title") or "",
                "content": content_clean[:800],
                "time": time_str,
                "publish_time": dt_obj,
                "level": it.get("level") or "C",
                "subjects": [{"name": s.get("subject_name", "")} for s in subjects[:5]],
                "stocks": stock_info,
                "reading_num": it.get("reading_num") or 0,
                "is_top": bool(it.get("is_top")),
                "jpush": bool(it.get("jpush")),
                "bold": bool(it.get("bold")),
                "author": it.get("author") or "",
            })
        last_time = roll_data[-1].get("ctime") or 0
        if not last_time:
            break
    return items


def _fetch_eastmoney_news(client, pages=1):
    items = []
    for page in range(1, pages + 1):
        url = f"https://np-listapi.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_724&fastColumn=102&sortEnd=&pageSize=20&page={page}&req_trace=1"
        r = client.get(url, timeout=NEWS_REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status()
        fl = (r.json().get("data") or {}).get("fastNewsList") or []
        if not fl:
            break
        for it in fl:
            ts_str = it.get("showTime") or ""
            dt_obj = None
            if ts_str:
                from datetime import datetime as _dt
                try:
                    dt_obj = _dt.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                except Exception as exc:
                    _record_fallback('_fetch_eastmoney_news:1686', exc)
            stock_raw = it.get("stockList") or []
            stock_info = []
            for s in stock_raw:
                if isinstance(s, str) and "." in s:
                    parts = s.split(".")
                    if len(parts) == 2:
                        stock_info.append({"name": "", "code": parts[1]})
            items.append({
                "source": "eastmoney",
                "source_id": str(it.get("code") or ""),
                "title": it.get("title") or "",
                "content": it.get("summary") or "",
                "time": ts_str,
                "publish_time": dt_obj,
                "level": "C",
                "subjects": [],
                "stocks": stock_info,
                "reading_num": 0,
                "is_top": False,
                "jpush": False,
                "bold": False,
                "author": "东方财富",
            })
    return items


def _fetch_sina_news(client, pages=1):
    items = []
    for page in range(1, pages + 1):
        url = f"https://zhibo.sina.com.cn/api/zhibo/feed?page={page}&page_size=20&zhibo_id=152&tag_id=0&type=0"
        r = client.get(url, timeout=NEWS_REQUEST_TIMEOUT_SECONDS)
        r.raise_for_status()
        feed = (r.json().get("result") or {}).get("data") or {}
        lst = (feed.get("feed") or {}).get("list") or []
        if not lst:
            break
        for it in lst:
            ts_str = it.get("create_time") or ""
            dt_obj = None
            if ts_str:
                from datetime import datetime as _dt
                try:
                    dt_obj = _dt.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                except Exception as exc:
                    _record_fallback('_fetch_sina_news:1731', exc)
            tag_list = it.get("tag") or []
            subjects = [{"name": t.get("name", "")} for t in tag_list if t.get("name")]
            stock_info = []
            ext_raw = it.get("ext") or ""
            if ext_raw:
                import json as _json
                try:
                    ext = _json.loads(ext_raw) if isinstance(ext_raw, str) else ext_raw
                    for s in (ext.get("stocks") or []):
                        if isinstance(s, dict) and s.get("symbol"):
                            stock_info.append({"name": s.get("key", ""), "code": s.get("symbol", "")})
                except Exception as exc:
                    _record_fallback('_fetch_sina_news:1744', exc)
            items.append({
                "source": "sina",
                "source_id": str(it.get("id") or ""),
                "title": "",
                "content": it.get("rich_text") or "",
                "time": ts_str,
                "publish_time": dt_obj,
                "level": "C",
                "subjects": subjects,
                "stocks": stock_info,
                "reading_num": 0,
                "is_top": bool(it.get("top_value")),
                "jpush": False,
                "bold": False,
                "author": "新浪财经",
            })
    return items


def _calc_importance(it: dict) -> int:
    score = 0
    if it.get("level") in ("A", "B"):
        score += 3 if it["level"] == "A" else 2
    if it.get("jpush"):
        score += 2
    if it.get("is_top"):
        score += 1
    if it.get("bold"):
        score += 1
    rn = it.get("reading_num") or 0
    if rn >= 50000:
        score += 2
    elif rn >= 10000:
        score += 1
    text_to_check = ((it.get("title") or "") + (it.get("content") or "")).lower()
    for kw in IMPORTANT_KEYWORDS:
        if kw.lower() in text_to_check:
            score += 1
            break
    if it.get("stocks"):
        score += 1
    return score


IMPORTANT_KEYWORDS = [
    "央行", "降息", "加息", "降准", "LPR", "MLF", "逆回购",
    "中美", "关税", "贸易", "制裁",
    "国务院", "证监会", "发改委", "财政部", "工信部",
    "GDP", "CPI", "PPI", "PMI", "社融", "M2",
    "战争", "冲突", "封锁", "军事",
    "涨停", "跌停", "熔断",
    "退市", "ST", "暴雷", "违约",
    "利率", "汇率", "外汇",
    "重大", "突发", "紧急",
]


def _merge_and_dedup(all_items: list) -> list:
    def _title_key(t: str) -> str:
        import re as _re
        t = _re.sub(r"[【】\s\u3000]+", "", t)
        return t[:40]
    groups: dict[str, list] = {}
    for it in all_items:
        key = _title_key(it.get("title") or it.get("content") or "")
        if not key:
            key = f"_unique_{it['source']}_{it['source_id']}"
        groups.setdefault(key, []).append(it)
    merged = []
    for key, group in groups.items():
        if len(group) == 1:
            item = group[0].copy()
            item["sources"] = [group[0]["source"]]
        else:
            best = max(group, key=lambda x: _calc_importance(x))
            item = best.copy()
            item["sources"] = list(dict.fromkeys(g["source"] for g in group))
            all_stocks = []
            seen_codes = set()
            for g in group:
                for s in (g.get("stocks") or []):
                    c = s.get("code", "")
                    if c and c not in seen_codes:
                        seen_codes.add(c)
                        all_stocks.append(s)
            item["stocks"] = all_stocks
            best_content = max((g.get("content") or "" for g in group), key=len)
            item["content"] = best_content
        item["importance_score"] = _calc_importance(item)
        merged.append(item)
    merged.sort(key=lambda x: (x.get("publish_time") or datetime.min), reverse=True)
    return merged


def _save_news_to_db(engine, items: list):
    from datetime import datetime as _dt
    import json as _json
    etl = _dt.now().replace(microsecond=0)
    upsert = text(
        "INSERT INTO st_news_flash (source, source_id, title, content, publish_time, first_seen_at, level, stocks, subjects, reading_num, is_top, jpush, extra, etl_sync_at) "
        "VALUES (:source, :source_id, :title, :content, :publish_time, :etl_sync_at, :level, :stocks, :subjects, :reading_num, :is_top, :jpush, :extra, :etl_sync_at) "
        "ON DUPLICATE KEY UPDATE title=VALUES(title), content=VALUES(content), level=VALUES(level), etl_sync_at=VALUES(etl_sync_at)"
    )
    saved = 0
    try:
        with engine.begin() as conn:
            for it in items:
                try:
                    conn.execute(upsert, {
                        "source": it["source"],
                        "source_id": it["source_id"],
                        "title": (it.get("title") or "")[:512],
                        "content": it.get("content") or "",
                        "publish_time": it.get("publish_time"),
                        "level": it.get("level") or "C",
                        "stocks": _json.dumps(it.get("stocks") or [], ensure_ascii=False),
                        "subjects": _json.dumps(it.get("subjects") or [], ensure_ascii=False),
                        "reading_num": it.get("reading_num") or 0,
                        "is_top": 1 if it.get("is_top") else 0,
                        "jpush": 1 if it.get("jpush") else 0,
                        "extra": None,
                        "etl_sync_at": etl,
                    })
                    saved += 1
                except Exception as exc:
                    _record_fallback('_save_news_to_db:1870', exc)
    except Exception as exc:
        _record_fallback('_save_news_to_db:1872', exc)
    return saved


@router.get("/hot-data/news-flash")
def news_flash(
    rn: int = Query(default=200),
    pages: int = Query(default=3, ge=1, le=5),
    source: str = Query(default="all"),
):
    """多源快讯聚合: cls/eastmoney/sina/all"""
    try:
        import httpx
        all_items = []
        with httpx.Client(headers={"User-Agent": "Mozilla/5.0 ProBigA"}, timeout=10.0) as client:
            if source in ("all", "cls"):
                try:
                    all_items.extend(_fetch_cls_news(client, pages))
                except Exception as exc:
                    _record_fallback('news_flash:1891', exc)
            if source in ("all", "eastmoney"):
                try:
                    all_items.extend(_fetch_eastmoney_news(client, max(1, pages // 2)))
                except Exception as exc:
                    _record_fallback('news_flash:1896', exc)
            if source in ("all", "sina"):
                try:
                    all_items.extend(_fetch_sina_news(client, max(1, pages // 2)))
                except Exception as exc:
                    _record_fallback('news_flash:1901', exc)

        if source == "all":
            merged = _merge_and_dedup(all_items)
        else:
            for it in all_items:
                it["sources"] = [it["source"]]
                it["importance_score"] = _calc_importance(it)
            merged = sorted(all_items, key=lambda x: (x.get("publish_time") or datetime.min), reverse=True)

        saved = _save_news_to_db(get_engine(), all_items)

        result = []
        for it in merged[:rn]:
            it.pop("publish_time", None)
            result.append(it)
        return {"data": result, "total": len(result), "saved": saved}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/news-important")
def news_important(pages: int = Query(default=2, ge=1, le=5)):
    """多源重要快讯"""
    try:
        import httpx
        all_items = []
        with httpx.Client(headers={"User-Agent": "Mozilla/5.0 ProBigA"}, timeout=10.0) as client:
            try:
                all_items.extend(_fetch_cls_news(client, pages))
            except Exception as exc:
                _record_fallback('news_important:1932', exc)
            try:
                all_items.extend(_fetch_eastmoney_news(client, max(1, pages // 2)))
            except Exception as exc:
                _record_fallback('news_important:1936', exc)
            try:
                all_items.extend(_fetch_sina_news(client, max(1, pages // 2)))
            except Exception as exc:
                _record_fallback('news_important:1940', exc)

        merged = _merge_and_dedup(all_items)
        important = [it for it in merged if it.get("importance_score", 0) >= 2]
        for it in important:
            it.pop("publish_time", None)
        return {"data": important, "total": len(important)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/news-history")
def news_history(source: str = Query(default=""), limit: int = Query(default=200)):
    """查询历史快讯（从数据库）"""
    try:
        if source and source != "all":
            rows = _read_sql(
                "SELECT source, source_id, title, content, publish_time, level, stocks, subjects, reading_num, is_top, jpush "
                "FROM st_news_flash WHERE source = :s ORDER BY publish_time DESC LIMIT :n",
                {"s": source, "n": min(limit, 500)}
            )
        else:
            rows = _read_sql(
                "SELECT source, source_id, title, content, publish_time, level, stocks, subjects, reading_num, is_top, jpush "
                "FROM st_news_flash ORDER BY publish_time DESC LIMIT :n",
                {"n": min(limit, 500)}
            )
        for r in rows:
            r["sources"] = [r.get("source", "")]
        return {"data": rows, "total": len(rows)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


@router.get("/hot-data/stock-notices")
def stock_notices(
    stock_code: str = Query(default=""),
    limit: int = Query(default=50),
    include_future: bool = Query(default=False),
):
    """查询个股公告（si_notice_eastmoney）"""
    try:
        future_clause = "" if include_future else " AND notice_date <= CURDATE()"
        try:
            limit_n = min(max(1, int(limit)), 200)
        except Exception:
            limit_n = 50
        if stock_code.strip():
            rows = _read_sql(
                "SELECT stock_code, notice_date, title, column_name, display_time, detail_url "
                f"FROM si_notice_eastmoney WHERE stock_code = :c AND association_validated = 1{future_clause} "
                "ORDER BY notice_date DESC LIMIT :n",
                {"c": stock_code.strip().zfill(6), "n": limit_n}
            )
        else:
            rows = _read_sql(
                "SELECT stock_code, notice_date, title, column_name, display_time, detail_url "
                f"FROM si_notice_eastmoney WHERE association_validated = 1{future_clause} ORDER BY notice_date DESC LIMIT :n",
                {"n": limit_n}
            )
        return {"data": rows, "total": len(rows), "include_future": bool(include_future)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


def _compute_indicators(codes: list, td: str) -> dict:
    """批量计算MACD(EMA)+KDJ+MA，返回 {code: {ema12,ema26,dif,dea,hist,k,d,j,ma5,ma10,ma20}}"""
    if not codes:
        return {}
    import math as _math
    engine = get_engine()
    placeholders = ",".join([f":c{i}" for i in range(len(codes))])
    params = {f"c{i}": c for i, c in enumerate(codes)}
    params["d"] = td

    sql = f"""
        SELECT stock_code, trade_date, close, high, low FROM sm_stock_kline
        WHERE stock_code IN ({placeholders}) AND k_type=1 AND adjust_type=0
          AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 80 DAY) AND :d
        ORDER BY stock_code, trade_date
    """
    rows = _read_sql(sql, params)
    by_code = {}
    for r in rows:
        code = str(r["stock_code"])
        by_code.setdefault(code, []).append((str(r["trade_date"]), float(r["close"] or 0), float(r["high"] or 0), float(r["low"] or 0)))

    result = {}
    ema12_k = 2.0 / 13.0
    ema26_k = 2.0 / 27.0
    dea_k = 2.0 / 10.0

    for code, series in by_code.items():
        if len(series) < 26:
            continue
        closes = [s[1] for s in series]
        highs = [s[2] for s in series]
        lows = [s[3] for s in series]
        dates = [s[0] for s in series]

        # Find index of td
        try:
            tdi = dates.index(td)
        except ValueError:
            continue
        if tdi < 26:
            continue

        # EMA12, EMA26
        ema12 = closes[0]
        ema26 = closes[0]
        ema12_prev = ema12
        ema26_prev = ema26
        dea_vals = []
        for i in range(1, tdi + 1):
            ema12 = closes[i] * ema12_k + ema12_prev * (1 - ema12_k)
            ema26 = closes[i] * ema26_k + ema26_prev * (1 - ema26_k)
            ema12_prev = ema12
            ema26_prev = ema26
            dif = ema12 - ema26
            if i == 25:
                dea = dif
            elif i > 25:
                dea = dif * dea_k + dea_vals[-1] * (1 - dea_k)
            else:
                continue
            dea_vals.append(dea)

        dif_now = ema12 - ema26
        dea_now = dea_vals[-1] if dea_vals else dif_now
        hist_now = (dif_now - dea_now) * 2

        # MACD金叉: DIF上穿DEA(前一日DIF<DEA, 今日DIF>DEA)
        if tdi >= 27 and len(dea_vals) >= 2:
            dif_prev = closes[tdi - 1]  # approximate with previous dif
            # recompute prev dif
            ema12_p = closes[0]
            ema26_p = closes[0]
            for i in range(1, tdi):
                ema12_p = closes[i] * ema12_k + ema12_p * (1 - ema12_k)
                ema26_p = closes[i] * ema26_k + ema26_p * (1 - ema26_k)
            dif_prev = ema12_p - ema26_p
            dea_prev = dea_vals[-2] if len(dea_vals) >= 2 else dea_vals[-1]
            golden_cross = dif_prev <= dea_prev and dif_now > dea_now
        else:
            golden_cross = dif_now > dea_now

        # KDJ (9,3,3): compute RSV, then K/D/J with EMA-style smoothing
        kdj_n = 9
        if tdi >= kdj_n:
            rsv_list = []
            for j in range(max(0, tdi - 20), tdi + 1):
                h9j = max(highs[max(0, j - kdj_n + 1):j + 1])
                l9j = min(lows[max(0, j - kdj_n + 1):j + 1])
                rsv_j = (closes[j] - l9j) / (h9j - l9j) * 100 if h9j > l9j else 50.0
                rsv_list.append(rsv_j)
            k_vals = [50.0]
            d_vals = [50.0]
            for rsv_j in rsv_list:
                k_vals.append(2.0/3 * k_vals[-1] + 1.0/3 * rsv_j)
                d_vals.append(2.0/3 * d_vals[-1] + 1.0/3 * k_vals[-1])
            k_val = round(k_vals[-1], 1)
            d_val = round(d_vals[-1], 1)
            j_val = round(3 * k_val - 2 * d_val, 1)
        else:
            k_val = 50.0
            d_val = 50.0
            j_val = 50.0

        # MA5, MA10, MA20
        ma5 = sum(closes[max(0, tdi - 4):tdi + 1]) / min(5, tdi + 1)
        ma10 = sum(closes[max(0, tdi - 9):tdi + 1]) / min(10, tdi + 1)
        ma20 = sum(closes[max(0, tdi - 19):tdi + 1]) / min(20, tdi + 1)

        result[code] = {
            "ema12": round(ema12, 2), "ema26": round(ema26, 2),
            "dif": round(dif_now, 2), "dea": round(dea_now, 2),
            "hist": round(hist_now, 2), "golden_cross": golden_cross,
            "k": round(k_val, 1), "d": round(d_val, 1), "j": round(j_val, 1),
            "ma5": round(ma5, 2), "ma10": round(ma10, 2), "ma20": round(ma20, 2),
            "close": closes[tdi],
        }
    return result


def _latest_date(table: str, col: str = "trade_date") -> str:
    cache_key = f"latest_date_{table}_{col}"
    cached = _cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        rows = _read_sql(f"SELECT MAX({quote_identifier(col)}) AS md FROM {quote_identifier(table)}", {})
        if rows and rows[0].get("md"):
            result = str(rows[0]["md"])
            _cache_set(cache_key, result)
            return result
    except Exception as exc:
        logger.debug("failed to enrich live rank short names: %s", exc)
    return date.today().isoformat()


def _latest_date_not_after(table: str, requested: str, col: str = "trade_date") -> str:
    requested = str(requested or "").strip()[:10]
    if not requested:
        return _latest_date(table, col)
    cache_key = f"latest_date_not_after_{table}_{col}_{requested}"
    cached = _cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        quoted_table = quote_identifier(table)
        quoted_col = quote_identifier(col)
        rows = _read_sql(
            f"SELECT {quoted_col} AS d FROM {quoted_table} WHERE {quoted_col} <= :d ORDER BY {quoted_col} DESC LIMIT 1",
            {"d": requested},
        )
        if rows and rows[0].get("d"):
            result = str(rows[0]["d"])
            _cache_set(cache_key, result)
            return result
    except Exception as exc:
        logger.debug("failed to enrich live rank quotes from Sina: %s", exc)
    return ""


def _latest_market_analysis_date(requested: str = "") -> str:
    """Return the latest available market date across snapshot and daily K-line."""
    requested = str(requested or "").strip()[:10]
    dates: list[str] = []
    for table in ("sm_stock_snapshot", "sm_stock_kline"):
        try:
            d = _latest_date_not_after(table, requested) if requested else _latest_date(table)
        except Exception:
            d = ""
        d = str(d or "")[:10]
        if d:
            dates.append(d)
    return max(dates) if dates else (requested or date.today().isoformat())


def _news_counts_for_codes(codes: list[str], trade_date: str) -> dict[str, int]:
    """Count recent news mentions with one query instead of one query per code."""
    normalized = list(dict.fromkeys(
        str(code or "").strip().zfill(6)
        for code in codes
        if str(code or "").strip()
    ))
    if not normalized:
        return {}
    rows = _read_sql(
        """
        SELECT stocks
        FROM st_news_flash
        WHERE publish_time >= DATE_SUB(:d, INTERVAL 3 DAY)
          AND publish_time < DATE_ADD(:d, INTERVAL 1 DAY)
          AND stocks IS NOT NULL
          AND stocks <> ''
        """,
        {"d": trade_date},
    )
    counts = {code: 0 for code in normalized}
    for row in rows:
        encoded_stocks = str(row.get("stocks") or "")
        for code in normalized:
            if code in encoded_stocks:
                counts[code] += 1
    return counts


@router.get("/hot-data/screen-stocks")
def screen_stocks(
    mode: str = Query(default="lhb"),
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
    top: int = Query(default=50, ge=1, le=200),
    min_change: float = Query(default=0, alias="min_chg"),
    max_change: float = Query(default=20, alias="max_chg"),
    min_turnover: float = Query(default=0, alias="min_tor"),
    min_main_flow: float = Query(default=1000000, alias="min_flow"),
    min_boards: int = Query(default=2, alias="min_b"),
    max_boards: int = Query(default=5, alias="max_b"),
    vol_boost: float = Query(default=1.2, alias="vboost"),
    max_from_low: float = Query(default=0.08, alias="max_dist"),
    low_lookback: int = Query(default=20, alias="lookback"),
    min_chg_trend: float = Query(default=-1, alias="min_trend"),
    limit_pct: float = Query(default=9.5, alias="limit"),
    trend_days: int = Query(default=5, alias="t_days", description="[trend_strong] 连续站上MA5最少天数"),
    ma_slope_min: float = Query(default=0.2, alias="slope", description="[trend_strong] MA20斜率下限%"),
    vol_ratio_min: float = Query(default=0.5, alias="vr_min", description="[trend_strong] 量比下限"),
    vol_ratio_max: float = Query(default=3.0, alias="vr_max", description="[trend_strong] 量比上限"),
    max_60d_gain: float = Query(default=200.0, alias="max_gain", description="[trend_strong] 60日最大涨幅%"),
    new_high_pct: float = Query(default=0.90, alias="nh_pct", description="[trend_strong] 距新高比例"),
):
    """选股策略筛选。

    ``trade_date`` is an as-of date: use the latest available data on or
    before the requested date and expose that fallback explicitly.  Older
    versions silently ignored the date picker and always used the global
    latest date, which made historical screening and reproducibility
    impossible.
    """

    requested_date = str(trade_date or "").strip()[:10]

    def screen_date(table: str) -> str:
        # An explicit as-of date must never fall forward to a newer data day.
        # If that table has no data on/before the requested date, keep the
        # empty date so the caller receives an unavailable/empty result.
        if requested_date:
            return _latest_date_not_after(table, requested_date)
        return _latest_date(table)

    try:
        if mode == "lhb":
            td = screen_date("st_a_list_daily")
            if not _read_sql(f"SELECT 1 FROM st_a_list_daily WHERE trade_date=:d LIMIT 1", {"d": td}):
                return {
                    "mode": mode,
                    "date": td,
                    "requested_date": requested_date,
                    "data_date": td,
                    "freshness": "unavailable" if not td else ("exact" if not requested_date or td == requested_date else "fallback"),
                    "data": [],
                    "total": 0,
                    "note": "龙虎榜无数据",
                }
            sql = """
                SELECT d.stock_code, COALESCE(NULLIF(d.short_name,''), c.short_name) AS short_name,
                       d.change_cpt AS change_pct, d.turnover_ratio,
                       d.a_net_amount, d.reason
                FROM st_a_list_daily d LEFT JOIN si_all_code c ON c.stock_code = d.stock_code
                WHERE d.trade_date = :d ORDER BY ABS(d.change_cpt) DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "lim": top})
            for row in rows:
                net = float(row.get("a_net_amount") or 0)
                row["lhb_direction"] = (
                    "NET_BUY" if net > 0 else ("NET_SELL" if net < 0 else "NEUTRAL")
                )
                row["lhb_direction_score"] = 100.0 if net > 0 else (0.0 if net < 0 else 50.0)
        elif mode == "flow":
            td = screen_date("sm_stock_capital_flow_daily")
            sql = """
                SELECT f.stock_code, c.short_name, f.main_net_inflow, f.max_net_inflow
                FROM sm_stock_capital_flow_daily f LEFT JOIN si_all_code c ON c.stock_code = f.stock_code
                WHERE f.trade_date = :d AND f.main_net_inflow >= :m
                ORDER BY f.main_net_inflow DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "m": min_main_flow, "lim": top})
        elif mode == "k_day":
            td = screen_date("sm_stock_kline")
            sql = """
                SELECT k.stock_code, COALESCE(NULLIF(k.short_name,''), c.short_name) AS short_name,
                       k.change_pct, k.turnover_ratio, k.close, k.amount
                FROM sm_stock_kline k
                LEFT JOIN si_all_code c ON k.stock_code = c.stock_code
                WHERE k.trade_date = :d AND k.k_type=1 AND k.adjust_type=0
                  AND k.change_pct >= :cmin AND k.change_pct <= :cmax
                  AND (k.turnover_ratio IS NULL OR k.turnover_ratio >= :tmin)
                ORDER BY k.change_pct DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "cmin": min_change, "cmax": max_change, "tmin": min_turnover, "lim": top})
        elif mode == "low_start":
            td = screen_date("sm_stock_kline")
            sql = f"""
                SELECT k.stock_code, COALESCE(NULLIF(k.short_name,''), c.short_name) AS short_name,
                       k.close, k.change_pct, k.turnover_ratio,
                       ROUND((k.close - hl.min_low_before) / NULLIF(hl.min_low_before, 0) * 100, 2) AS dist_from_low_pct,
                       ROUND(k.volume / NULLIF(hv.avg_vol_20_before, 0), 2) AS vol_ratio
                FROM sm_stock_kline k
                LEFT JOIN si_all_code c ON k.stock_code = c.stock_code
                INNER JOIN (SELECT k1.stock_code, MIN(k1.low) AS min_low_before
                    FROM sm_stock_kline k1 WHERE k1.k_type=1 AND k1.adjust_type=0
                      AND k1.trade_date < :d AND k1.trade_date >= DATE_SUB(:d, INTERVAL {low_lookback} DAY)
                    GROUP BY k1.stock_code) hl ON k.stock_code = hl.stock_code
                INNER JOIN (SELECT k2.stock_code, AVG(k2.volume) AS avg_vol_20_before
                    FROM sm_stock_kline k2 WHERE k2.k_type=1 AND k2.adjust_type=0
                      AND k2.trade_date < :d AND k2.trade_date >= DATE_SUB(:d, INTERVAL 20 DAY)
                    GROUP BY k2.stock_code) hv ON k.stock_code = hv.stock_code
                WHERE k.trade_date = :d AND k.k_type=1 AND k.adjust_type=0
                  AND k.change_pct >= :cmin AND k.change_pct <= :cmax
                  AND (k.close - hl.min_low_before) / NULLIF(hl.min_low_before, 0) <= :mxdist
                  AND k.volume >= :vboost * hv.avg_vol_20_before
                ORDER BY k.change_pct DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "cmin": min_change, "cmax": max_change,
                                   "mxdist": max_from_low, "vboost": vol_boost, "lim": top})
        elif mode == "trend":
            td = screen_date("sm_stock_kline")
            sql = """
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio,
                       ROUND(ma5.avg_c,2) AS ma5_c, ROUND(ma10.avg_c,2) AS ma10_c,
                       ROUND(ma20.avg_c,2) AS ma20_c,
                       ROUND((t.close / NULLIF(ma20.avg_c, 0) - 1) * 100, 2) AS ma_spread_pct
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 5 DAY) AND :d
                  GROUP BY stock_code
                ) ma5 ON t.stock_code = ma5.stock_code
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 10 DAY) AND :d
                  GROUP BY stock_code
                ) ma10 ON t.stock_code = ma10.stock_code
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 20 DAY) AND :d
                  GROUP BY stock_code
                ) ma20 ON t.stock_code = ma20.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND ma5.avg_c > ma10.avg_c AND ma10.avg_c > ma20.avg_c AND t.close > ma5.avg_c
                  AND t.change_pct >= :cmin
                ORDER BY ma_spread_pct DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "cmin": min_chg_trend, "lim": top})
        elif mode == "trend_strong":
            td = screen_date("sm_stock_kline")
            # 第一步: SQL筛选四线多头排列 + 基础过滤
            sql = """
                SELECT t.stock_code,
                       COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close, 2) AS close, t.change_pct, t.turnover_ratio,
                       ROUND(ma5.v, 2) AS ma5, ROUND(ma10.v, 2) AS ma10,
                       ROUND(ma20.v, 2) AS ma20, ROUND(ma60.v, 2) AS ma60
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                INNER JOIN (
                    SELECT stock_code, AVG(close) AS v FROM sm_stock_kline
                    WHERE k_type=1 AND adjust_type=0
                      AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 5 DAY)
                    GROUP BY stock_code HAVING COUNT(*) >= 4
                ) ma5 ON t.stock_code = ma5.stock_code
                INNER JOIN (
                    SELECT stock_code, AVG(close) AS v FROM sm_stock_kline
                    WHERE k_type=1 AND adjust_type=0
                      AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 10 DAY)
                    GROUP BY stock_code HAVING COUNT(*) >= 8
                ) ma10 ON t.stock_code = ma10.stock_code
                INNER JOIN (
                    SELECT stock_code, AVG(close) AS v FROM sm_stock_kline
                    WHERE k_type=1 AND adjust_type=0
                      AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 20 DAY)
                    GROUP BY stock_code HAVING COUNT(*) >= 14
                ) ma20 ON t.stock_code = ma20.stock_code
                INNER JOIN (
                    SELECT stock_code, AVG(close) AS v FROM sm_stock_kline
                    WHERE k_type=1 AND adjust_type=0
                      AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 60 DAY)
                    GROUP BY stock_code HAVING COUNT(*) >= 30
                ) ma60 ON t.stock_code = ma60.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%%ST%%'
                  AND t.short_name NOT LIKE '%%ST%%'
                  AND ma5.v > ma10.v AND ma10.v > ma20.v AND ma20.v > ma60.v
                  AND t.close > ma5.v
                ORDER BY t.close / NULLIF(ma60.v, 0) DESC
                LIMIT :scan_lim
            """
            raw_rows = _read_sql(sql, {"d": td, "scan_lim": top})
            if not raw_rows:
                rows = []
            else:
                # 第二步: Python批量补算趋势指标
                codes = [str(r["stock_code"]).strip().zfill(6) for r in raw_rows]
                ph = ",".join(f"'{c}'" for c in codes)
                hist_sql = f"""
                    SELECT stock_code, trade_date, close, high, low, volume
                    FROM sm_stock_kline
                    WHERE stock_code IN ({ph}) AND k_type=1 AND adjust_type=0
                      AND trade_date <= '{td}' AND trade_date > DATE_SUB('{td}', INTERVAL 80 DAY)
                    ORDER BY stock_code, trade_date DESC
                """
                hist = _read_sql(hist_sql)
                from collections import defaultdict
                code_hist = defaultdict(list)
                for h in hist:
                    code_hist[str(h["stock_code"]).strip().zfill(6)].append(h)

                rows = []
                for r in raw_rows:
                    code = str(r["stock_code"]).strip().zfill(6)
                    grp = code_hist.get(code, [])
                    if len(grp) < 20:
                        continue
                    closes = [float(x["close"] or 0) for x in grp]
                    highs = [float(x["high"] or 0) for x in grp]
                    volumes = [float(x["volume"] or 0) for x in grp]

                    # 连续站上MA5天数
                    above_ma5_days = 0
                    for i in range(min(len(closes), 60)):
                        win = closes[i:i + 5]
                        if len(win) < 5:
                            break
                        if closes[i] >= sum(win) / 5:
                            above_ma5_days += 1
                        else:
                            break

                    # 60日指标
                    lb = min(len(closes), 60)
                    high_60 = max(highs[:lb])
                    close_now = closes[0]
                    close_60ago = closes[lb - 1] if lb > 0 else close_now
                    gain_60d = (close_now - close_60ago) / close_60ago * 100 if close_60ago > 0 else 0
                    near_high_pct = close_now / high_60 if high_60 > 0 else 0

                    # 量比
                    vol_5 = sum(volumes[:5]) / min(5, len(volumes[:5])) if volumes[:5] else 0
                    vol_20 = sum(volumes[:20]) / min(20, len(volumes[:20])) if volumes[:20] else 0
                    vol_ratio = vol_5 / vol_20 if vol_20 > 0 else 0

                    # MA20斜率
                    ma20_now_v = sum(closes[:20]) / 20 if len(closes) >= 20 else 0
                    ma20_prev_v = sum(closes[10:30]) / 20 if len(closes) >= 30 else 0
                    ma20_slope = (ma20_now_v - ma20_prev_v) / ma20_prev_v * 100 if ma20_prev_v > 0 else 0

                    # 过滤
                    if above_ma5_days < trend_days:
                        continue
                    if ma20_slope < ma_slope_min:
                        continue
                    if near_high_pct < new_high_pct:
                        continue
                    if vol_ratio < vol_ratio_min or vol_ratio > vol_ratio_max:
                        continue
                    if gain_60d > max_60d_gain or gain_60d <= 0:
                        continue

                    r["above_ma5_days"] = above_ma5_days
                    r["gain_60d"] = round(gain_60d, 2)
                    r["near_high_pct"] = round(near_high_pct * 100, 1)
                    r["vol_ratio"] = round(vol_ratio, 2)
                    r["ma20_slope_pct"] = round(ma20_slope, 2)
                    r["high_60d"] = round(high_60, 2)
                    rows.append(r)

                rows.sort(key=lambda x: (-x["above_ma5_days"], -x["gain_60d"]))
                rows = rows[:top]
        elif mode == "ladder":
            td = screen_date("sm_stock_kline")
            # 第一步: 获取今日涨停股
            today_limit = _read_sql("""
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                  AND t.change_pct >= 4.5
            """, {"d": td})
            tolerance = max(0.8, min(1.0, float(limit_pct) / 10.0))
            today_limit = [
                row
                for row in today_limit
                if float(row.get("change_pct") or 0)
                >= board_limit_trigger_pct(
                    row.get("stock_code"),
                    row.get("short_name"),
                    tolerance=tolerance,
                )
            ]
            if not today_limit:
                rows = []
            else:
                # 第二步: 获取这些股票近20日K线，Python计算连板数
                codes = [str(r["stock_code"]) for r in today_limit]
                ph = ",".join([f"'{c}'" for c in codes])
                hist = _read_sql(f"""
                    SELECT stock_code, trade_date, change_pct
                    FROM sm_stock_kline
                    WHERE stock_code IN ({ph}) AND k_type=1 AND adjust_type=0
                      AND trade_date BETWEEN DATE_SUB('{td}', INTERVAL 30 DAY) AND '{td}'
                    ORDER BY stock_code, trade_date DESC
                """)
                from collections import defaultdict
                hist_map = defaultdict(list)
                for h in hist:
                    hist_map[str(h["stock_code"])].append(h)

                rows = []
                for r in today_limit:
                    code = str(r["stock_code"])
                    trigger_pct = board_limit_trigger_pct(
                        code, r.get("short_name"), tolerance=tolerance
                    )
                    boards = 1
                    for h in hist_map.get(code, []):
                        if str(h["trade_date"]) == str(td):
                            continue
                        if float(h["change_pct"] or 0) >= trigger_pct:
                            boards += 1
                        else:
                            break
                    if min_boards <= boards <= max_boards:
                        r["boards"] = boards
                        r["limit_trigger_pct"] = trigger_pct
                        rows.append(r)
                rows.sort(key=lambda x: (-x["boards"], -float(x["change_pct"] or 0)))
                rows = rows[:top]
        elif mode == "macd":
            td = screen_date("sm_stock_kline")
            # Use the shared real EMA/MACD/KDJ implementation.  The old
            # endpoint used simple averages for EMA and hard-coded K/D/J=50.
            base_rows = _read_sql(
                """
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%*ST%'
                ORDER BY t.change_pct DESC
                LIMIT :lim
                """,
                {"d": td, "lim": max(top * 8, 200)},
            )
            indicators = _compute_indicators([str(r["stock_code"]) for r in base_rows], td)
            rows = []
            for r in base_rows:
                ind = indicators.get(str(r["stock_code"]))
                if not ind or not ind.get("golden_cross"):
                    continue
                r.update(ind)
                rows.append(r)
            rows.sort(key=lambda x: (-(float(x.get("hist") or 0)), -(float(x.get("change_pct") or 0))))
            rows = rows[:top]
        elif mode == "startup":
            td = screen_date("sm_stock_kline")
            sql = """
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio,
                       ROUND(ma20.avg_c,2) AS ma20,
                       ROUND(t.volume / NULLIF(base.avg_vol, 0), 1) AS vol_ratio,
                       ROUND((t.close - base.max_high) / NULLIF(base.max_high, 0) * 100, 1) AS breakout_pct,
                       ROUND((base.max_high - base.min_low) / NULLIF(base.min_low, 0) * 100, 1) AS range_width_pct,
                       COALESCE(f.main_net_inflow, 0) AS main_net_inflow
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                LEFT JOIN sm_stock_capital_flow_daily f ON t.stock_code = f.stock_code AND f.trade_date = :d
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 20 DAY) AND :d
                  GROUP BY stock_code
                ) ma20 ON t.stock_code = ma20.stock_code
                INNER JOIN (
                  SELECT stock_code, MIN(low) AS min_low, MAX(high) AS max_high, AVG(volume) AS avg_vol
                  FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0
                    AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 15 DAY) AND DATE_SUB(:d, INTERVAL 2 DAY)
                  GROUP BY stock_code
                ) base ON t.stock_code = base.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%*ST%'
                  AND t.close > ma20.avg_c
                  AND t.close < ma20.avg_c * 1.2
                  AND t.volume > base.avg_vol * :vboost
                  AND t.close >= base.max_high
                  AND (base.max_high - base.min_low) / NULLIF(base.min_low, 0) < 0.18
                  AND t.change_pct >= :cmin AND t.change_pct <= :cmax
                ORDER BY COALESCE(f.main_net_inflow, 0) DESC, t.volume / NULLIF(base.avg_vol, 0) DESC
                LIMIT :lim
            """
            rows = _read_sql(
                sql,
                {
                    "d": td,
                    "lim": top,
                    "vboost": vol_boost,
                    "cmin": min_change,
                    "cmax": max_change,
                },
            )
            if rows:
                codes = [str(r["stock_code"]).strip().zfill(6) for r in rows]
                inds = _compute_indicators(codes, td)
                try:
                    news_map = _news_counts_for_codes(codes, td)
                except Exception as exc:
                    _record_fallback('screen_stocks:startup_news', exc)
                    news_map = {}
                for r in rows:
                    code = str(r["stock_code"]).strip().zfill(6)
                    ind = inds.get(code, {})
                    r["k"] = ind.get("k", 50)
                    r["d"] = ind.get("d", 50)
                    r["j"] = ind.get("j", 50)
                    r["dif"] = ind.get("dif", 0)
                    r["dea"] = ind.get("dea", 0)
                    r["macd_golden"] = ind.get("golden_cross", False)
                    r["news_count"] = news_map.get(code, 0)
        else:
            return {"mode": mode, "data": [], "total": 0, "error": f"未知模式: {mode}"}

        effective_date = td if "td" in locals() else (requested_date or trade_date)
        return {
            "mode": mode,
            "date": effective_date,
            "requested_date": requested_date,
            "data_date": effective_date,
            "freshness": "unavailable" if not effective_date else ("exact" if not requested_date or effective_date == requested_date else "fallback"),
            "data": rows,
            "total": len(rows),
        }
    except Exception as e:
        return {
            "mode": mode,
            "date": requested_date or trade_date,
            "requested_date": requested_date,
            "data_date": requested_date or trade_date,
            "freshness": "error",
            "data": [],
            "total": 0,
            "error": str(e),
        }


@router.get("/hot-data/ai-screen")
def ai_screen(
    query: str = Query(default="", description="自然语言选股描述"),
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
    top: int = Query(default=20, ge=1, le=50),
):
    """AI智能选股：多轮纠错 + 自动重试"""
    if not query.strip():
        return {"data": [], "total": 0, "error": "请输入选股描述"}

    api_key = (os.environ.get("DEEPSEEK_API_KEY", "").strip() or _read_dotenv_key())
    if not api_key:
        return {"data": [], "total": 0, "error": "未配置DeepSeek API Key"}

    td = _latest_date_inline("sm_stock_kline")
    TOP = top

    import httpx, re as _re

    _SCHEMA_PROMPT = """你是MySQL 5.7专家。生成SQL必须完全用标准MySQL 5.7语法。

【数据库表】
sm_stock_kline: stock_code short_name trade_date open close high low volume amount change_pct turnover_ratio k_type
  用途: 个股日K线(k_type=1)+技术指标计算
sm_stock_capital_flow_daily: stock_code trade_date main_net_inflow lg_net_inflow mid_net_inflow sm_net_inflow
  用途: 个股日度资金流向(主力/大单/中单/小单)
si_concept_constituent_ths: stock_code short_name query_key query_type
si_concept_code_ths: name index_code concept_code source
  用途: 概念成分股。JOIN: ct.query_key=cc.index_code AND ct.query_type='index_code'
st_news_flash: title content publish_time level stocks
  用途: 快讯/新闻
si_all_code: stock_code short_name
  用途: 全量代码表

【技术指标 MySQL 5.7 兼容公式】
MA5: (SELECT AVG(k2.close) FROM sm_stock_kline k2 WHERE k2.stock_code=k.stock_code AND k2.k_type=1 AND k2.trade_date<=k.trade_date AND k2.trade_date>=DATE_SUB(k.trade_date,INTERVAL 4 DAY))
MA10: 把INTERVAL 4 DAY改成INTERVAL 9 DAY
MA20: 把INTERVAL 4 DAY改成INTERVAL 19 DAY
MA60: 把INTERVAL 4 DAY改成INTERVAL 59 DAY

"近N天站在MA5上方": 窗口用INTERVAL (N+5) DAY覆盖周末。统计每天close>MA5的天数。
(SELECT COUNT(*) FROM sm_stock_kline kd WHERE kd.stock_code=k.stock_code AND kd.k_type=1 AND kd.trade_date<=k.trade_date AND kd.trade_date>=DATE_SUB(k.trade_date,INTERVAL {N_DAYS} DAY) AND kd.close > (SELECT AVG(kp.close) FROM sm_stock_kline kp WHERE kp.stock_code=kd.stock_code AND kp.k_type=1 AND kp.trade_date<=kd.trade_date AND kp.trade_date>=DATE_SUB(kd.trade_date,INTERVAL 4 DAY))) AS above_days
N=10时 N_DAYS=15, N=5时 N_DAYS=9, N=20时 N_DAYS=28

"近N天站在MA5上方/下方": HAVING above_days >= N（无结果时用>=N-2）
"MACD金叉": DIF上穿DEA → 当日DIF>DEA 且 前一日DIF<=DEA
  子查询DIF: (SELECT AVG(k2.close)*12/13 - AVG(k3.close)*... 太复杂，改用:
  EXISTS(SELECT 1 FROM sm_stock_kline kp WHERE kp.stock_code=k.stock_code AND kp.k_type=1 AND kp.trade_date=DATE_SUB(k.trade_date,INTERVAL 1 DAY) AND kp.change_pct > 0)
  简化: MACD金叉→近期从跌转涨，用change_pct>=3 AND 前一日<=0近似
"放量上涨": volume > (SELECT AVG(k2.volume) FROM sm_stock_kline k2 WHERE k2.stock_code=k.stock_code AND k2.k_type=1 AND k2.trade_date<k.trade_date AND k2.trade_date>=DATE_SUB(k.trade_date,INTERVAL 5 DAY)) * 1.5 AND k.change_pct > 0
"突破前高": close > (SELECT MAX(k2.high) FROM sm_stock_kline k2 WHERE k2.stock_code=k.stock_code AND k2.k_type=1 AND k2.trade_date<k.trade_date AND k2.trade_date>=DATE_SUB(k.trade_date,INTERVAL 20 DAY))
"连续上涨": 要求change_pct>0 连续N天:
(SELECT COUNT(*) FROM sm_stock_kline kd WHERE kd.stock_code=k.stock_code AND kd.k_type=1 AND kd.trade_date<=k.trade_date AND kd.trade_date>=DATE_SUB(k.trade_date,INTERVAL {UP_DAYS_WIN} DAY) AND kd.change_pct>0) AS up_days  HAVING up_days >= N

【概念子查询】(用户提概念/板块才用)
概念筛选: EXISTS(SELECT 1 FROM si_concept_constituent_ths ct JOIN si_concept_code_ths cc ON ct.query_key=cc.index_code AND ct.query_type='index_code' WHERE ct.stock_code=k.stock_code AND cc.name LIKE '%关键词%')
概念列表: (SELECT GROUP_CONCAT(DISTINCT cc.name SEPARATOR ',') FROM si_concept_constituent_ths ct JOIN si_concept_code_ths cc ON ct.query_key=cc.index_code AND ct.query_type='index_code' WHERE ct.stock_code=k.stock_code) AS concept_names

【必须条件】
k.k_type=1 AND k.trade_date=:d
k.stock_code REGEXP '^(0|60)' AND k.short_name NOT LIKE '%ST%'

【输出格式】纯JSON:
{{"sql":"SELECT ...", "reasons":{{"股票代码":"15字原因"}}}}

用户需求: {query}
当前最新交易日: {td}"""

    def _call_deepseek(messages):
        resp = httpx.post(
            "https://api.deepseek.com/v1/chat/completions",
            json={"model": "deepseek-chat", "messages": messages,
                  "temperature": 0.3, "max_tokens": 4096},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
        )
        if resp.status_code != 200:
            return None, f"AI调用失败: {resp.status_code}"
        content = resp.json()["choices"][0]["message"]["content"]
        content = content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()
        m = _re.search(r'\{[\s\S]*\}', content)
        if not m:
            return None, f"AI返回格式异常: {content[:300]}"
        try:
            parsed = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            return None, f"JSON解析失败: {e}"
        return parsed, ""

    msg_hist = [
        {"role": "system", "content": "你是MySQL 5.7专家，只输出JSON，不解释。SQL要能直接在MySQL 5.7执行。"},
        {"role": "user", "content": _SCHEMA_PROMPT.format(query=query.strip(), N_DAYS="15", N=10, UP_DAYS_WIN="19", td=td)},
    ]

    rows = []
    reasons = {}
    final_sql = ""
    note = ""

    for round_num in range(3):
        parsed, err = _call_deepseek(msg_hist)
        if err:
            return {"data": [], "total": 0, "error": err}

        sql = parsed.get("sql", "")
        reasons = parsed.get("reasons", {})
        if not sql:
            msg_hist.append({"role": "assistant", "content": json.dumps(parsed, ensure_ascii=False)})
            msg_hist.append({"role": "user", "content": "你没生成sql字段，请重新生成包含sql的JSON"})
            continue

        final_sql = sql
        try:
            rows = _read_sql(sql, {"d": td})
        except Exception as e:
            err_msg = str(e)[:300]
            if round_num < 2:
                msg_hist.append({"role": "assistant", "content": json.dumps(parsed, ensure_ascii=False)})
                msg_hist.append({"role": "user", "content": f"SQL执行报错: {err_msg}。请修正SQL重新生成，必须用MySQL 5.7语法。检查：1)INTERVAL值是否正确 2)列名是否存在 3)子查询括号是否匹配 4)HAVING是否在正确位置"})
                continue
            return {"data": [], "total": 0, "error": f"SQL执行失败(已重试{round_num+1}次): {err_msg}", "sql": sql}

        if rows:
            break

        if round_num == 0:
            msg_hist.append({"role": "assistant", "content": json.dumps(parsed, ensure_ascii=False)})
            msg_hist.append({"role": "user", "content": f"SQL执行成功但返回0条结果。请放宽筛选条件（如降低天数阈值、去掉非核心约束、把>=N改为>=N-2），重新生成SQL。"})
            continue
        else:
            note = "已尝试放宽条件仍未找到完全匹配的股票，建议换一种描述方式"
            break

    for r in rows:
        code = str(r.get("stock_code", "")).strip()
        short = str(r.get("short_name", "")).strip()
        reason = reasons.get(code, "")
        if not reason:
            for k in reasons:
                if k in code or code in k or (short and k in short):
                    reason = reasons[k]
                    break
        r["ai_reason"] = reason or "符合条件"

    if not rows and not note:
        note = "当天市场暂无完全匹配的股票，建议放宽条件或换种描述"

    return {"mode": "ai", "date": td, "query": query, "data": rows, "total": len(rows), "sql": final_sql, "note": note}


def _latest_date_inline(table: str, col: str = "trade_date") -> str:
    try:
        rows = _read_sql(f"SELECT MAX({quote_identifier(col)}) AS md FROM {quote_identifier(table)}", {})
        if rows and rows[0].get("md"):
            return str(rows[0]["md"])
    except Exception as exc:
        _record_fallback('_latest_date_inline:2722', exc)
    return date.today().isoformat()


@router.get("/hot-data/stock-minute")
def stock_minute(stock_code: str = Query(), trade_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """获取个股当天分时走势"""
    try:
        source = minute_source_info()
        rows = get_stock_minute_prices(stock_code, trade_date, trade_date)
        if rows:
            return {"stock_code": stock_code, "date": trade_date, "data": rows, "total": len(rows), "source": source}
        # 如果数据库没有，尝试实时接口
        try:
            from adata.stock.market.stock_market.stock_market import StockMarket
            sm = StockMarket()
            df = sm.get_market_min(stock_code=stock_code)
            if df is not None and not df.empty:
                df = df.replace({np.nan: None, pd.NaT: None})
                for c in df.columns:
                    if df[c].dtype == "datetime64[ns]":
                        df[c] = df[c].astype(str)
                records = df.to_dict(orient="records")
                return {"stock_code": stock_code, "date": trade_date, "data": records, "total": len(records), "source": "api"}
        except Exception as exc:
            _record_fallback('stock_minute:2747', exc)
        return {"stock_code": stock_code, "date": trade_date, "data": [], "total": 0, "source": source}
    except Exception as e:
        return {"stock_code": stock_code, "date": trade_date, "data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 全市场股票 API
# ═══════════════════════════════════════════

@router.get("/hot-data/stock-list")
def stock_list(
    keyword: str = Query(default=""),
    price: float = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=10, le=200),
    sort: str = Query(default="change_pct"),
    order: str = Query(default="desc"),
):
    """全市场股票列表，支持关键词搜索和按金额筛选，自选股置顶"""
    try:
        # 排序字段白名单（基于 sm_stock_snapshot 列名）
        sort_map = {
            "change_pct": "s.change_pct",
            "amount": "s.amount",
            "turnover_ratio": "s.turnover_ratio",
            "main_net_inflow": "s.main_net_inflow",
            "market_cap": "s.market_cap",
            "close": "s.price",
            "stock_code": "s.stock_code",
            "short_name": "s.short_name",
            "change_3d": "s.change_3d",
            "change_5d": "s.change_5d",
            "change_10d": "s.change_10d",
        }
        sort_col = sort_map.get(sort, sort_map["change_pct"])
        sort_dir = "DESC" if order.lower() == "desc" else "ASC"

        where_parts = ["1=1"]
        params = {}

        if keyword:
            kw = keyword.strip()
            where_parts.append("(s.stock_code LIKE :kw OR s.short_name LIKE :kw)")
            params["kw"] = f"%{kw}%"

        if price is not None and price > 0:
            where_parts.append("s.close BETWEEN :p_lo AND :p_hi")
            params["p_lo"] = round(price * 0.995, 2)
            params["p_hi"] = round(price * 1.005, 2)

        where_sql = " AND ".join(where_parts)

        # 统计总数
        count_sql = f"SELECT COUNT(*) AS cnt FROM sm_stock_snapshot s WHERE {where_sql}"
        cnt_rows = _read_sql(count_sql, params)
        total = int(cnt_rows[0]["cnt"]) if cnt_rows and cnt_rows[0].get("cnt") else 0

        # 分页查询，自选股置顶
        offset = (page - 1) * page_size
        params["limit"] = page_size
        params["offset"] = offset
        data_sql = f"""
            SELECT
                s.stock_code, s.short_name, s.trade_date,
                s.open, s.close, s.high, s.low, s.pre_close, s.price,
                s.`change`, s.change_pct, s.volume, s.amount, s.turnover_ratio,
                s.main_net_inflow, s.max_net_inflow, s.lg_net_inflow,
                s.mid_net_inflow, s.sm_net_inflow,
                s.change_3d, s.change_5d, s.change_10d,
                s.market_cap, s.industry,
                s.sort_order, s.is_holding
            FROM sm_stock_snapshot s
            WHERE {where_sql}
            ORDER BY s.sort_order IS NULL, s.sort_order ASC, {sort_col} {sort_dir}
            LIMIT :limit OFFSET :offset
        """
        rows = _read_sql(data_sql, params)

        trade_date_rows = _read_sql("SELECT trade_date FROM sm_stock_snapshot LIMIT 1")
        trade_date = str(trade_date_rows[0]["trade_date"]) if trade_date_rows else ""

        return {
            "date": trade_date,
            "page": page,
            "page_size": page_size,
            "total": total,
            "data": rows,
        }
    except Exception as e:
        return {"date": "", "page": page, "page_size": page_size, "total": 0, "data": [], "error": str(e)}


@router.get("/hot-data/analysis-result")
def get_analysis_result(
    stock_code: str = Query(default=""),
    status: str = Query(default=""),
    min_short_score: float = Query(default=0),
    min_long_score: float = Query(default=0),
    sort_by: str = Query(default="short_term_score"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=10, le=200),
):
    """查询统一分析结果"""
    try:
        # 获取最新分析日期
        date_rows = _read_sql("SELECT MAX(analysis_date) AS d FROM stock_analysis_result")
        analysis_date = date_rows[0]["d"] if date_rows and date_rows[0].get("d") else None

        if not analysis_date:
            return {"date": "", "page": page, "page_size": page_size, "total": 0, "data": []}

        # 构建查询条件
        conditions = ["analysis_date = :date"]
        params = {"date": analysis_date}

        if stock_code:
            code = stock_code.strip().zfill(6)
            conditions.append("(stock_code = :code OR stock_name LIKE :name)")
            params["code"] = code
            params["name"] = f"%{stock_code}%"

        if status:
            conditions.append("recommend_status = :status")
            params["status"] = status

        if min_short_score > 0:
            conditions.append("short_term_score >= :min_st")
            params["min_st"] = min_short_score

        if min_long_score > 0:
            conditions.append("long_term_score >= :min_lt")
            params["min_lt"] = min_long_score

        where_clause = " AND ".join(conditions)

        # 排序
        valid_sort_fields = ["short_term_score", "long_term_score", "event_risk_score", "analysis_date"]
        if sort_by not in valid_sort_fields:
            sort_by = "short_term_score"
        order_clause = f"{sort_by} DESC"

        # 查询总数
        count_sql = f"SELECT COUNT(*) AS total FROM stock_analysis_result WHERE {where_clause}"
        count_rows = _read_sql(count_sql, params)
        total = count_rows[0]["total"] if count_rows else 0

        columns = _table_columns("stock_analysis_result")

        # 分页查询
        offset = (page - 1) * page_size
        query_sql = f"""
            SELECT {_analysis_result_select_list(columns)}
            FROM stock_analysis_result r
            WHERE {where_clause}
            ORDER BY {order_clause}
            LIMIT :limit OFFSET :offset
        """
        params["limit"] = page_size
        params["offset"] = offset

        rows = _read_sql(query_sql, params)

        # 解析JSON字段
        import json
        for row in rows:
            for json_field in ["event_risk_detail", "strengths", "risks", "data_quality_flags"]:
                if row.get(json_field) and isinstance(row[json_field], str):
                    try:
                        row[json_field] = json.loads(row[json_field])
                    except (TypeError, json.JSONDecodeError) as exc:
                        logger.debug("Failed to parse analysis JSON field %s: %s", json_field, exc)

        return {
            "date": str(analysis_date),
            "page": page,
            "page_size": page_size,
            "total": total,
            "data": rows,
        }
    except Exception as e:
        return {"date": "", "page": page, "page_size": page_size, "total": 0, "data": [], "error": str(e)}


@router.get("/hot-data/stock-detail")
def stock_detail(stock_code: str = Query()):
    """个股详情：7大模块投资决策数据"""
    code = stock_code.strip().zfill(6)
    mode = _portfolio_market_mode()
    cache_key = f"stock_detail_{code}_{mode}"
    cached = _cache_get(cache_key, ttl_seconds=_trading_live_ttl_seconds(300, intraday_seconds=12))
    if cached is not None:
        return cached
    try:
        try:
            payload = _load_stock_detail_payload(code, mode=mode, light=True)
        except Exception:
            payload = _load_stock_detail_payload(code, mode=mode, light=False)
        basic = payload.get("basic") or {}
        market = payload.get("market") or {}
        capital = payload.get("capital") or {}
        finance = payload.get("finance") or {}
        valuation = payload.get("valuation") or {}
        technical = payload.get("technical") or {}
        news_module = payload.get("news") or {}
        holder = payload.get("holder") or {}
        holding = payload.get("holding")
        industry = payload.get("industry")
        concepts = payload.get("concepts") or []
        trade_date = str(payload.get("trade_date") or "")
        requested_trade_date = str(payload.get("requested_trade_date") or trade_date or "")
        quote_trade_date = str(payload.get("quote_trade_date") or trade_date or "")
        flow_trade_date = str(payload.get("flow_trade_date") or quote_trade_date or "")
        analysis_snapshot = _load_latest_analysis_snapshot(code, trade_date=trade_date or None)
        recommendation_snapshot = _load_latest_recommendation_snapshot(code, trade_date=trade_date or None)
        ai_analysis = _generate_ai_analysis(
            code,
            basic.get("short_name"),
            market,
            capital,
            finance,
            valuation,
            technical,
            industry,
            concepts,
            holding,
            hot_rank=payload.get("hot_rank"),
            trade_date=trade_date,
            analysis_snapshot=analysis_snapshot,
            prefer_snapshot=True,
        )
        analysis_trade_date = ""
        if isinstance(ai_analysis, dict):
            analysis_trade_date = str(ai_analysis.get("analysis_date") or "")
        if not analysis_trade_date and isinstance(analysis_snapshot, dict):
            analysis_trade_date = str(analysis_snapshot.get("analysis_date") or "")
        data_mode_label = "盘中实时" if mode == "intraday" else "盘后收盘"

        result = {
            "stock_code": code,
            "short_name": basic.get("short_name"),
            "industry": industry,
            "concepts": concepts,
            "exchange": basic.get("exchange"),
            "list_date": str(basic.get("list_date", "")),
            "market": market,
            "capital": capital,
            "finance": finance,
            "valuation": valuation,
            "technical": technical,
            "news": news_module,
            "holder": holder,
            "analysis_snapshot": analysis_snapshot,
            "recommendation_snapshot": recommendation_snapshot,
            "ai_analysis": ai_analysis,
            "holding": holding,
            "mode": mode,
            "data_mode_label": data_mode_label,
            "date": trade_date,
            "requested_trade_date": requested_trade_date,
            "quote_trade_date": quote_trade_date,
            "flow_trade_date": flow_trade_date,
            "analysis_trade_date": analysis_trade_date,
            "quote_source": payload.get("quote_source") or "",
            "detail_source": payload.get("detail_source") or "",
            "quote_is_stale": bool(requested_trade_date and quote_trade_date and quote_trade_date < requested_trade_date),
            "flow_is_stale": bool(requested_trade_date and flow_trade_date and flow_trade_date < requested_trade_date),
            "analysis_is_stale": bool(requested_trade_date and analysis_trade_date and analysis_trade_date < requested_trade_date),
        }
        if not result.get("error"):
            _cache_set(cache_key, result)
        return result
    except Exception as e:
        return {"stock_code": stock_code, "error": str(e)}


def _compute_technical(kline_data, cur_price):
    """计算技术指标：MA/MACD/KDJ/RSI/BOLL/支撑压力"""
    if not kline_data or len(kline_data) < 20:
        return {}

    # kline_data 按日期倒序，转为正序计算
    rows = list(reversed(kline_data))
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    n = len(closes)

    # 均线
    def ma(data, period):
        if len(data) < period:
            return None
        return round(sum(data[-period:]) / period, 2)

    ma5 = ma(closes, 5)
    ma10 = ma(closes, 10)
    ma20 = ma(closes, 20)
    ma60 = ma(closes, 60)
    ma120 = ma(closes, 120)
    ma250 = ma(closes, 250)

    # MACD (12, 26, 9)
    ema12 = closes[0]
    ema26 = closes[0]
    dif_list = []
    dea = 0
    for c in closes:
        ema12 = ema12 * 11 / 13 + c * 2 / 13
        ema26 = ema26 * 25 / 27 + c * 2 / 27
        dif = ema12 - ema26
        dea = dea * 8 / 10 + dif * 2 / 10
        dif_list.append({"dif": round(dif, 4), "dea": round(dea, 4), "hist": round((dif - dea) * 2, 4)})

    macd_cur = dif_list[-1] if dif_list else {}
    macd_prev = dif_list[-2] if len(dif_list) >= 2 else {}
    golden_cross = (macd_prev.get("dif", 0) < macd_prev.get("dea", 0) and
                    macd_cur.get("dif", 0) > macd_cur.get("dea", 0))

    # KDJ (9, 3, 3)
    k, d = 50, 50
    for i in range(8, n):
        period_high = max(highs[i - 8:i + 1])
        period_low = min(lows[i - 8:i + 1])
        if period_high == period_low:
            rsv = 50
        else:
            rsv = (closes[i] - period_low) / (period_high - period_low) * 100
        k = k * 2 / 3 + rsv / 3
        d = d * 2 / 3 + k / 3
    j = 3 * k - 2 * d

    # RSI (6, 12, 24)
    def calc_rsi(data, period):
        if len(data) < period + 1:
            return None
        gains, losses = [], []
        for i in range(len(data) - period, len(data)):
            diff = data[i] - data[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return round(100 - 100 / (1 + rs), 2)

    rsi6 = calc_rsi(closes, 6)
    rsi12 = calc_rsi(closes, 12)
    rsi24 = calc_rsi(closes, 24)

    # BOLL (20, 2)
    if n >= 20:
        boll_mid = sum(closes[-20:]) / 20
        variance = sum((c - boll_mid) ** 2 for c in closes[-20:]) / 20
        std = variance ** 0.5
        boll_upper = round(boll_mid + 2 * std, 2)
        boll_lower = round(boll_mid - 2 * std, 2)
        boll_mid = round(boll_mid, 2)
    else:
        boll_upper = boll_mid = boll_lower = None

    # 支撑位和压力位
    # 短期：近5日最低/最高（适合趋势中的回调/突破判断）
    # 中期：近20日最低/最高（大区间参考）
    recent_lows_5 = lows[-5:] if len(lows) >= 5 else lows
    recent_highs_5 = highs[-5:] if len(highs) >= 5 else highs
    recent_lows_20 = lows[-20:] if len(lows) >= 20 else lows
    recent_highs_20 = highs[-20:] if len(highs) >= 20 else highs
    support = round(min(recent_lows_5), 2)
    resistance = round(max(recent_highs_5), 2)
    support_mid = round(min(recent_lows_20), 2)
    resistance_mid = round(max(recent_highs_20), 2)

    # 趋势判断
    short_trend = "上涨" if ma5 and ma10 and ma5 > ma10 else "下跌" if ma5 and ma10 else "震荡"
    mid_trend = "上涨" if ma20 and ma60 and ma20 > ma60 else "下跌" if ma20 and ma60 else "震荡"
    long_trend = "上涨" if ma120 and ma250 and ma120 > ma250 else "下跌" if ma120 and ma250 else "震荡"

    return {
        "ma": {"ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60, "ma120": ma120, "ma250": ma250},
        "macd": {**macd_cur, "golden_cross": golden_cross},
        "kdj": {"k": round(k, 2), "d": round(d, 2), "j": round(j, 2)},
        "rsi": {"rsi6": rsi6, "rsi12": rsi12, "rsi24": rsi24},
        "boll": {"upper": boll_upper, "mid": boll_mid, "lower": boll_lower},
        "support": support,
        "resistance": resistance,
        "support_mid": support_mid,
        "resistance_mid": resistance_mid,
        "trend": {"short": short_trend, "mid": mid_trend, "long": long_trend},
    }


def _legacy_capital_view(capital: dict | None) -> dict:
    """Convert loader capital values back to legacy yuan units for existing pages."""
    capital = capital or {}
    out = dict(capital)
    today = dict(out.get("today") or {})
    for key in ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"):
        if today.get(key) is not None:
            today[key] = float(today[key]) * 10000
    out["today"] = today
    for key in ("flow_3d", "flow_5d", "flow_20d"):
        if out.get(key) is not None:
            out[key] = float(out[key]) * 10000
    return out


def _parse_snapshot_json_fields(row: dict | None, json_fields: tuple[str, ...]) -> dict | None:
    if not row:
        return None
    parsed = dict(row)
    for field in json_fields:
        value = parsed.get(field)
        if isinstance(value, str) and value:
            try:
                parsed[field] = json.loads(value)
            except Exception as exc:
                _record_fallback('_parse_snapshot_json_fields:3165', exc)
    for field in ("analysis_date", "flow_trade_date", "hot_trade_date", "last_news_time"):
        if parsed.get(field) is not None:
            parsed[field] = str(parsed[field])
    return parsed


def _load_latest_analysis_snapshot(stock_code: str, trade_date: str | None = None) -> dict | None:
    columns = _table_columns("stock_analysis_result")
    sql = """
        SELECT {select_list}
        FROM stock_analysis_result r
        WHERE stock_code = :stock_code
    """.format(select_list=_analysis_result_select_list(columns))
    params = {"stock_code": stock_code}
    if trade_date:
        sql += " AND analysis_date <= :trade_date"
        params["trade_date"] = trade_date
    sql += " ORDER BY analysis_date DESC LIMIT 1"
    rows = _read_sql(sql, params)
    if not rows:
        return None
    snapshot = _parse_snapshot_json_fields(
        rows[0],
        ("event_risk_detail", "strengths", "risks", "data_quality_flags"),
    )
    if snapshot and snapshot.get("stock_name") and not snapshot.get("short_name"):
        snapshot["short_name"] = snapshot.get("stock_name")
    return snapshot


def _as_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _load_latest_recommendation_snapshot(stock_code: str, trade_date: str | None = None) -> dict | None:
    columns = _table_columns("st_recommended_stocks")
    if not columns:
        return None
    select_cols = [
        _select_col(columns, "stock_code", "''"),
        _select_col(columns, "short_name", "''"),
        _select_col(columns, "pick_date", "NULL"),
        _select_col(columns, "ai_score", "NULL"),
        _select_col(columns, "final_trade_score", "NULL"),
        _select_col(columns, "quality_score", "NULL"),
        _select_col(columns, "entry_score", "NULL"),
        _select_col(columns, "risk_reward_ratio", "NULL"),
        _select_col(columns, "sector_gate_status", "''"),
        _select_col(columns, "sector_gate_reason", "''"),
        _select_col(columns, "sector_flow_3d", "NULL"),
        _select_col(columns, "sector_width_pct", "NULL"),
        _select_col(columns, "technical_evidence_json", "'{}'"),
        _select_col(columns, "evidence_chain_json", "'[]'"),
        _select_col(columns, "review_1d_pct", "NULL"),
        _select_col(columns, "review_3d_pct", "NULL"),
        _select_col(columns, "review_5d_pct", "NULL"),
        _select_col(columns, "review_10d_pct", "NULL"),
        _select_col(columns, "failure_tags_json", "'[]'"),
        _select_col(columns, "short_term_score", "NULL"),
        _select_col(columns, "long_term_score", "NULL"),
        _select_col(columns, "capital_score", "NULL"),
        _select_col(columns, "chip_capital_score", "NULL"),
        _select_col(columns, "technical", "NULL"),
        _select_col(columns, "signal_status", "''"),
        _select_col(columns, "recommend_status", "''"),
        _select_col(columns, "recommend_reason", "''"),
        _select_col(columns, "investment_rating", "'中性'"),
        _select_col(columns, "rating_reason", "''"),
        _select_col(columns, "primary_strategy", "''"),
        _select_col(columns, "model_version", "''"),
        _select_col(columns, "last_check_time", "NULL"),
        _select_col(columns, "created_at", "NULL"),
    ]
    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM st_recommended_stocks r
        WHERE r.stock_code = :stock_code
    """
    params = {"stock_code": stock_code}
    if trade_date:
        sql += " AND r.pick_date <= :trade_date"
        params["trade_date"] = trade_date
    order_cols = ["r.pick_date DESC"]
    if "last_check_time" in columns:
        order_cols.append("r.last_check_time DESC")
    if "created_at" in columns:
        order_cols.append("r.created_at DESC")
    sql += f" ORDER BY {', '.join(order_cols)} LIMIT 1"
    rows = _read_sql(sql, params)
    if not rows:
        return None
    out = dict(rows[0])
    out = _parse_snapshot_json_fields(
        out,
        ("technical_evidence_json", "evidence_chain_json", "failure_tags_json"),
    ) or out
    for field in ("pick_date", "last_check_time", "created_at"):
        if out.get(field) is not None:
            out[field] = str(out[field])
    final_score = _as_float(out.get("final_trade_score"))
    ai_score = _as_float(out.get("ai_score"))
    out["recommendation_score"] = final_score if final_score is not None else ai_score
    out["recommendation_score_source"] = "final_trade_score" if final_score is not None else "ai_score"
    return out


def _avg_scores(*values):
    numbers = [float(v) for v in values if _as_float(v) is not None]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 1)


def _localize_recommend_status_label(status: str) -> str:
    mapping = {
        "ALLOW": "可跟踪",
        "BLOCK": "回避",
        "SUSPENDED": "观察",
        "WATCH": "观察",
        "CONFIRM": "确认",
        "BUY_READY": "买入就绪",
        "SELL_ALERT": "卖出提醒",
    }
    return mapping.get(str(status or "").upper(), str(status or "-"))


def _localize_risk_level_label(level: str) -> str:
    mapping = {
        "LOW": "低",
        "MEDIUM": "中",
        "HIGH": "高",
        "CRITICAL": "极高",
    }
    return mapping.get(str(level or "").upper(), str(level or "-"))


def _build_snapshot_ai_analysis(
    code,
    name,
    market,
    technical,
    holding=None,
    trade_date: str = "",
    analysis_snapshot: dict | None = None,
    fallback_reason: str = "",
):
    snapshot = analysis_snapshot or {}
    if not snapshot:
        reason = fallback_reason or "未配置 DEEPSEEK_API_KEY，且当前没有可用的综合分析快照。"
        return {
            "source": "snapshot_fallback",
            "analysis_date": trade_date or "",
            "score": None,
            "scores": None,
            "action": "观望",
            "action_reason": reason,
            "conclusion": reason,
        }

    short_score = _as_float(snapshot.get("short_term_score"))
    long_score = _as_float(snapshot.get("long_term_score"))
    event_score = _as_float(snapshot.get("event_risk_score"))
    quality_score = _as_float(snapshot.get("data_quality_score"))
    overall_score = _avg_scores(short_score, long_score)
    if overall_score is None:
        overall_score = _avg_scores(short_score, long_score, event_score, quality_score)

    recommend_status = str(snapshot.get("recommend_status") or "SUSPENDED").upper()
    risk_level = str(snapshot.get("event_risk_level") or "LOW").upper()
    status_label = _localize_recommend_status_label(recommend_status)
    risk_label = _localize_risk_level_label(risk_level)
    has_position = bool(holding and int(holding.get("shares") or 0) > 0)
    if risk_level == "CRITICAL" or recommend_status == "BLOCK":
        action = "减仓" if has_position else "回避"
    elif recommend_status == "ALLOW":
        action = "持有" if has_position else "关注"
    else:
        action = "持有观察" if has_position else "观望"

    action_reason = (
        str(snapshot.get("recommend_reason") or "").strip()
        or str(snapshot.get("recommendation") or "").strip()
        or str(snapshot.get("summary") or "").strip()
        or fallback_reason
        or "当前建议来自综合分析快照回退结果。"
    )
    strengths = [str(item) for item in (snapshot.get("strengths") or []) if str(item).strip()]
    risks = [str(item) for item in (snapshot.get("risks") or []) if str(item).strip()]
    quality_flags = [str(item) for item in (snapshot.get("data_quality_flags") or []) if str(item).strip()]
    analysis_date = str(snapshot.get("analysis_date") or trade_date or "")

    price = _as_float((market or {}).get("price"))
    support = _as_float((technical or {}).get("support"))
    resistance = _as_float((technical or {}).get("resistance"))

    lines = [f"统一分析快照回退，基于 {analysis_date or '最近可用日期'} 的正式分析结果。"]
    if snapshot.get("summary"):
        lines.append(f"摘要：{snapshot.get('summary')}")
    if snapshot.get("recommendation"):
        lines.append(f"建议：{snapshot.get('recommendation')}")
    lines.append(
        "状态："
        f"{status_label}，风险等级 {risk_label}，"
        f"短线 {short_score if short_score is not None else '-'}，"
        f"长线 {long_score if long_score is not None else '-'}，"
        f"事件 {event_score if event_score is not None else '-'}，"
        f"数据质量 {quality_score if quality_score is not None else '-'}。"
    )
    if price is not None or support is not None or resistance is not None:
        lines.append(
            "位置："
            f"现价 {price if price is not None else '-'}，"
            f"支撑 {support if support is not None else '-'}，"
            f"压力 {resistance if resistance is not None else '-'}。"
        )
    if strengths:
        lines.append(f"优势：{'；'.join(strengths[:4])}")
    risk_parts = risks[:4]
    if quality_flags:
        risk_parts.extend([f"数据标记 {flag}" for flag in quality_flags[:3]])
    if risk_parts:
        lines.append(f"风险：{'；'.join(risk_parts)}")
    if has_position:
        shares = int(holding.get("shares") or 0)
        cost = _as_float(holding.get("cost_price"))
        if shares > 0:
            holding_line = f"持仓：{shares}股"
            if cost is not None and price is not None and cost > 0:
                profit_pct = round((price / cost - 1) * 100, 2)
                holding_line += f"，成本 {cost}，现价 {price}，浮盈 {profit_pct}%"
            lines.append(holding_line)
    if fallback_reason:
        lines.append(f"说明：{fallback_reason}")

    return {
        "source": "snapshot_fallback",
        "analysis_date": analysis_date,
        "score": overall_score,
        "scores": {
            "short_term_score": short_score,
            "long_term_score": long_score,
            "event_risk_score": event_score,
            "data_quality_score": quality_score,
        },
        "action": action,
        "action_reason": action_reason,
        "recommend_status": recommend_status,
        "event_risk_level": risk_level,
        "conclusion": "\n".join(line for line in lines if line),
    }


def _fmt_analysis_money_cn(value) -> str:
    n = _as_float(value)
    if n is None:
        return "-"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 100_000_000:
        return f"{sign}{n / 100_000_000:.2f}亿"
    if n >= 10_000:
        return f"{sign}{n / 10_000:.1f}万"
    return f"{sign}{n:.0f}"


def _fmt_analysis_num(value, digits: int = 2) -> str:
    n = _as_float(value)
    if n is None:
        return "-"
    return f"{n:.{digits}f}"


def _has_portfolio_analysis_sections(text_value: str) -> bool:
    text_value = str(text_value or "")
    return all(label in text_value for label in ("趋势判断", "资金态度", "热度评估", "操作建议", "风险提示"))


def _build_portfolio_section_analysis(
    *,
    market: dict,
    capital: dict,
    technical: dict,
    concepts: list,
    holding: dict | None,
    hot_rank: dict | None,
    ai_result: dict | None,
    analysis_snapshot: dict | None,
    recommendation_snapshot: dict | None,
) -> str:
    market = market or {}
    capital = capital or {}
    technical = technical or {}
    holding = holding or {}
    hot_rank = hot_rank or {}
    ai_result = ai_result or {}
    analysis_snapshot = analysis_snapshot or {}
    recommendation_snapshot = recommendation_snapshot or {}

    price = _as_float(market.get("price") or market.get("close"))
    change_pct = _as_float(market.get("change_pct"))
    ma = technical.get("ma") or {}
    trend = technical.get("trend") or {}
    support = _as_float(technical.get("support"))
    resistance = _as_float(technical.get("resistance"))
    main_flow = _as_float((capital.get("today") or {}).get("main_net_inflow"))
    rank = hot_rank.get("fused_rank") or hot_rank.get("rank") or analysis_snapshot.get("hot_rank")
    strengths = [str(item) for item in (analysis_snapshot.get("strengths") or []) if str(item).strip()]
    risks = [str(item) for item in (analysis_snapshot.get("risks") or []) if str(item).strip()]
    quality_flags = [str(item) for item in (analysis_snapshot.get("data_quality_flags") or []) if str(item).strip()]

    ma_parts = []
    for key in ("ma5", "ma10", "ma20"):
        if ma.get(key) is not None:
            ma_parts.append(f"{key.upper()}（{_fmt_analysis_num(ma.get(key))}）")
    if ma_parts:
        trend_text = f"现价{_fmt_analysis_num(price)}，涨跌幅{_fmt_analysis_num(change_pct)}%，位置参考{'、'.join(ma_parts)}。"
    else:
        trend_text = f"现价{_fmt_analysis_num(price)}，涨跌幅{_fmt_analysis_num(change_pct)}%，均线数据暂缺，先结合价格、资金和正式分析快照判断。"
    if trend:
        trend_text += f" 短期趋势{trend.get('short') or '-'}，中期趋势{trend.get('mid') or '-'}。"
    if support is not None or resistance is not None:
        trend_text += f" 参考支撑{_fmt_analysis_num(support)}，压力{_fmt_analysis_num(resistance)}。"

    if main_flow is not None:
        flow_word = "净流入" if main_flow >= 0 else "净流出"
        funds_text = f"今日主力资金{flow_word}{_fmt_analysis_money_cn(main_flow)}，"
        funds_text += "资金态度偏积极。" if main_flow >= 0 else "资金态度偏谨慎，需观察承接力度。"
    else:
        funds_text = "今日主力资金数据暂缺，资金态度先参考最近快照和盘面表现。"

    heat_bits = []
    if rank:
        heat_bits.append(f"融合热度第{rank}名")
    if concepts:
        heat_bits.append("概念：" + "、".join(str(x) for x in concepts[:4] if x))
    if strengths:
        heat_bits.append("亮点：" + "、".join(strengths[:3]))
    if analysis_snapshot.get("summary"):
        heat_bits.append(str(analysis_snapshot.get("summary")))
    heat_text = "，".join(heat_bits) + "。" if heat_bits else "暂无明确热度标签，按自选股跟踪级别观察。"

    action = str(ai_result.get("action") or "").strip()
    action_reason = (
        str(ai_result.get("action_reason") or "").strip()
        or str(recommendation_snapshot.get("recommend_reason") or "").strip()
        or str(analysis_snapshot.get("recommendation") or analysis_snapshot.get("recommend_reason") or "").strip()
    )
    if not action:
        action = "持有" if int(holding.get("shares") or 0) > 0 else "观望"
    advice_text = f"{action}。"
    shares = int(holding.get("shares") or 0)
    cost = _as_float(holding.get("cost_price"))
    if shares > 0:
        advice_text += f" 当前持有{shares}股"
        if cost is not None and price is not None and cost > 0:
            profit_pct = (price / cost - 1) * 100
            advice_text += f"，成本价{_fmt_analysis_num(cost)}元，现价{_fmt_analysis_num(price)}元，浮盈{profit_pct:.2f}%"
        advice_text += "。"
    if action_reason:
        advice_text += f" {action_reason}"
    elif support is not None:
        advice_text += f" 若跌破支撑{_fmt_analysis_num(support)}且放量，应优先控制仓位。"

    risk_parts = risks[:3]
    if quality_flags:
        risk_parts.extend([f"数据标记：{flag}" for flag in quality_flags[:2]])
    if change_pct is not None and abs(change_pct) >= 5:
        risk_parts.append("短线波动较大")
    if main_flow is not None and main_flow < 0:
        risk_parts.append("主力资金流出")
    if not risk_parts:
        risk_parts = ["短线涨跌幅扩大时注意获利盘或恐慌盘扰动", "若跌破关键均线/支撑位需重新评估"]

    return "\n\n".join([
        "### 趋势判断\n" + trend_text,
        "### 资金态度\n" + funds_text,
        "### 热度评估\n" + heat_text,
        "### 操作建议\n" + advice_text,
        "### 风险提示\n" + "；".join(risk_parts) + "。",
    ])


def _merge_ai_analysis_with_snapshot(
    ai_result: dict | None,
    code,
    name,
    market,
    technical,
    holding=None,
    trade_date: str = "",
    analysis_snapshot: dict | None = None,
):
    result = dict(ai_result or {})
    base = _build_snapshot_ai_analysis(
        code,
        name,
        market,
        technical,
        holding=holding,
        trade_date=trade_date,
        analysis_snapshot=analysis_snapshot,
    )
    if not analysis_snapshot:
        result.setdefault("source", "deepseek")
        result.setdefault("analysis_date", trade_date or "")
        return result
    merged = dict(base)
    merged.update({k: v for k, v in result.items() if v is not None})
    merged["source"] = result.get("source") or "deepseek+snapshot"
    merged["analysis_date"] = result.get("analysis_date") or base.get("analysis_date") or trade_date or ""
    return merged


def _load_stock_detail_payload(stock_code: str, mode: str | None = None, light: bool = False) -> dict:
    mode = mode or _portfolio_market_mode()
    trade_date = None if mode == "intraday" else _portfolio_close_trade_date()
    loader = StockDataLoader()
    if light:
        code = stock_code.strip().zfill(6)
        td = trade_date or _portfolio_close_trade_date()
        snapshot_trade_date = _latest_date_not_after("sm_stock_snapshot", td)
        basic_rows = _read_sql(
            "SELECT stock_code, short_name, exchange, list_date FROM si_all_code WHERE stock_code = :c",
            {"c": code},
        )
        basic = basic_rows[0] if basic_rows else {"stock_code": code, "short_name": code}
        industry_rows = _read_sql(
            "SELECT plate_name FROM si_stock_plate_east WHERE stock_code = :c AND plate_type = '行业' LIMIT 1",
            {"c": code},
        )
        concept_rows = _read_sql(
            "SELECT DISTINCT name FROM si_stock_concept_east WHERE stock_code = :c LIMIT 8",
            {"c": code},
        )
        market_rows = _read_sql(
            "SELECT price, close, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close, "
            "market_cap, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow "
            "FROM sm_stock_snapshot WHERE stock_code = :c AND trade_date = :td LIMIT 1",
            {"c": code, "td": snapshot_trade_date},
        )
        share_rows = _read_sql(
            "SELECT total_shares, list_a_shares FROM si_stock_shares WHERE stock_code = :c",
            {"c": code},
        )
        flow_td_rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily WHERE trade_date <= :td",
            {"td": td},
        )
        flow_trade_date = str(flow_td_rows[0]["d"])[:10] if flow_td_rows and flow_td_rows[0].get("d") else td
        flow_rows = _read_sql(
            "SELECT main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source "
            "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :td",
            {"c": code, "td": flow_trade_date},
        )
        fin_rows = _read_sql(
            "SELECT basic_eps, net_asset_ps, roe_wtd, roa_wtd, gross_margin, net_margin, "
            "total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr, report_date "
            "FROM si_stock_finance WHERE stock_code = :c AND report_date <= :td "
            "ORDER BY report_date DESC LIMIT 1",
            {"c": code, "td": td},
        )
        holder_rows = _read_sql(
            "SELECT report_date, holder_num, holder_num_change, pre_holder_num, holder_num_ratio, avg_free_shares "
            "FROM si_stock_holder WHERE stock_code = :c AND report_date <= :td "
            "ORDER BY report_date DESC LIMIT 1",
            {"c": code, "td": td},
        )
        quote = market_rows[0] if market_rows else {}
        shares = share_rows[0] if share_rows else {}
        fin = fin_rows[0] if fin_rows else {}
        holder = holder_rows[0] if holder_rows else {}
        price_val = float(quote.get("price") or 0)
        eps = float(fin.get("basic_eps") or 0)
        bvps = float(fin.get("net_asset_ps") or 0)
        total_shares = float(shares.get("total_shares") or 0)
        float_shares = float(shares.get("list_a_shares") or 0)
        payload = {
            "basic": basic,
            "market": {
                "price": quote.get("price"),
                "change_pct": quote.get("change_pct"),
                "open": quote.get("open"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "close": quote.get("price"),
                "pre_close": quote.get("pre_close"),
                "volume": quote.get("volume"),
                "amount": quote.get("amount"),
                "turnover_ratio": quote.get("turnover_ratio"),
                "total_shares": total_shares,
                "float_shares": float_shares,
                "market_cap": quote.get("market_cap") or (round(price_val * total_shares, 2) if price_val and total_shares else None),
                "float_market_cap": round(price_val * float_shares, 2) if price_val and float_shares else None,
            },
            "capital": {
                "today": ({
                    "main_net_inflow": float(flow_rows[0].get("main_net_inflow") or 0) / 10000,
                    "max_net_inflow": float(flow_rows[0].get("max_net_inflow") or 0) / 10000,
                    "lg_net_inflow": float(flow_rows[0].get("lg_net_inflow") or 0) / 10000,
                    "mid_net_inflow": float(flow_rows[0].get("mid_net_inflow") or 0) / 10000,
                    "sm_net_inflow": float(flow_rows[0].get("sm_net_inflow") or 0) / 10000,
                    "data_source": flow_rows[0].get("data_source") or "east",
                } if flow_rows else {}),
                "flow_3d": None,
                "flow_5d": None,
                "flow_20d": None,
                "dragon_tiger": {"count_20d": 0, "inst_net_buy": None, "seats": []},
            },
            "finance": {"latest": fin, "quarters": [fin] if fin else []},
            "valuation": {
                "pe_ttm": round(price_val / eps, 2) if eps and eps > 0 and price_val else None,
                "pe_percentile": None,
                "pb": round(price_val / bvps, 2) if bvps and bvps > 0 and price_val else None,
                "pb_percentile": None,
                "verdict": None,
            },
            "technical": {},
            "news": {"notices": [], "news": []},
            "holder": holder,
            "holding": None,
            "industry": industry_rows[0].get("plate_name") if industry_rows else None,
            "concepts": [row.get("name") for row in concept_rows if row.get("name")],
            "trade_date": snapshot_trade_date or td,
            "requested_trade_date": td,
            "quote_trade_date": snapshot_trade_date or td,
            "flow_trade_date": flow_trade_date,
            "quote_source": "snapshot",
            "detail_source": "snapshot_light",
            "hot_rank": {},
        }
    else:
        payload = loader.load_full_data(
            stock_code,
            trade_date=trade_date,
            use_realtime=(mode == "intraday"),
        )
    payload["capital"] = _legacy_capital_view(payload.get("capital"))
    return payload


def _generate_ai_analysis(
    code,
    name,
    market,
    capital,
    finance,
    valuation,
    technical,
    industry,
    concepts,
    holding=None,
    hot_rank=None,
    trade_date: str = "",
    analysis_snapshot: dict | None = None,
    prefer_snapshot: bool = False,
):
    """调用 DeepSeek 生成 AI 投资分析"""
    try:
        import httpx
        api_key = (os.getenv("DEEPSEEK_API_KEY", "").strip() or _read_dotenv_key())
        if not api_key:
            return _build_snapshot_ai_analysis(
                code,
                name,
                market,
                technical,
                holding=holding,
                trade_date=trade_date,
                analysis_snapshot=analysis_snapshot,
                fallback_reason="未配置 DEEPSEEK_API_KEY，当前改用分析快照回退结果。",
            )
        if prefer_snapshot and analysis_snapshot:
            return _build_snapshot_ai_analysis(
                code,
                name,
                market,
                technical,
                holding=holding,
                trade_date=trade_date,
                analysis_snapshot=analysis_snapshot,
                fallback_reason="当前详情页启用快照优先模式，以保证响应速度和稳定性。",
            )

        # 构建摘要给AI
        summary_parts = []
        summary_parts.append(f"股票：{name}({code})，行业：{industry}，概念：{', '.join(concepts[:5]) if concepts else '无'}")

        if market:
            summary_parts.append(f"现价：{market.get('price')}，涨跌幅：{market.get('change_pct')}%，PE：{market.get('pe_ttm')}，PB：{market.get('pb')}，量比：{market.get('volume_ratio')}")

        if capital.get("today"):
            t = capital["today"]
            summary_parts.append(f"今日主力净流入：{t.get('main_net_inflow')}，大单：{t.get('lg_net_inflow')}，中单：{t.get('mid_net_inflow')}")
        if capital.get("flow_5d"):
            summary_parts.append(f"5日主力净流入：{capital['flow_5d']}，20日：{capital.get('flow_20d')}")
        if capital.get("dragon_tiger", {}).get("count_20d"):
            summary_parts.append(f"近20日龙虎榜：{capital['dragon_tiger']['count_20d']}次")

        latest_fin = finance.get("latest", {})
        if latest_fin:
            summary_parts.append(f"最新报告期：{latest_fin.get('report_date')}，营收同比：{latest_fin.get('total_rev_yoy_gr')}%，净利润同比：{latest_fin.get('net_profit_yoy_gr')}%，ROE：{latest_fin.get('roe_wtd')}%，毛利率：{latest_fin.get('gross_margin')}%")

        if valuation:
            summary_parts.append(f"PE分位：{valuation.get('pe_percentile')}%，PB分位：{valuation.get('pb_percentile')}%，估值：{valuation.get('verdict')}")

        if technical:
            t = technical.get("trend", {})
            summary_parts.append(f"趋势-短期：{t.get('short')}，中期：{t.get('mid')}，长期：{t.get('long')}")
            summary_parts.append(f"支撑位：{technical.get('support')}，压力位：{technical.get('resistance')}")
            rsi = technical.get("rsi", {})
            summary_parts.append(f"RSI6：{rsi.get('rsi6')}，RSI12：{rsi.get('rsi12')}")
            ma = technical.get("ma", {})
            summary_parts.append(f"MA5：{ma.get('ma5')}，MA10：{ma.get('ma10')}，MA20：{ma.get('ma20')}")

        # 热门排名
        try:
            hr = hot_rank or {}
            if (not hr) and trade_date:
                hot_rows = _read_sql("""
                    SELECT fused_rank, east_rank, ths_rank, total_score
                    FROM st_hot_rank_fused
                    WHERE stock_code = :c AND snapshot_date <= :td
                    ORDER BY snapshot_date DESC LIMIT 1
                """, {"c": code, "td": trade_date})
                hr = hot_rows[0] if hot_rows else {}
            if hr:
                summary_parts.append(f"融合热度排名：第{hr.get('fused_rank')}名，东方财富排名：{hr.get('east_rank')}，同花顺排名：{hr.get('ths_rank')}")
        except Exception as exc:
            _record_fallback('_generate_ai_analysis:3799', exc)

        # 持仓信息
        holding_prompt = ""
        if holding and holding.get("shares") and int(holding["shares"]) > 0:
            shares = int(holding["shares"])
            cost = float(holding.get("cost_price") or 0)
            price = float(market.get("price") or 0)
            profit_pct = round((price / cost - 1) * 100, 2) if cost > 0 and price > 0 else 0
            profit_amt = round((price - cost) * shares, 2) if cost > 0 else 0
            holding_prompt = f"""
【持仓信息】该用户当前持有此股票：
- 持仓数量：{shares}股
- 成本价：{cost}元
- 现价：{price}元
- 持仓盈亏：{profit_pct}%（{profit_amt}元）
- 短期支撑位：{technical.get('support')}元
- 短期压力位：{technical.get('resistance')}元

请基于以上所有数据，以一个全球顶尖交易员的视角，给出明确的操作建议：持有、加仓、还是减仓。
考虑因素：当前位置相对成本价的盈亏、估值是否合理、资金面方向、技术面趋势、支撑压力位。
"""

        prompt = f"""你是一个全球顶尖的A股交易员和投资分析师。请根据以下数据，给出专业投资分析。你的分析要犀利、直接、有判断力，不要模棱两可。

{'chr(10)'.join(summary_parts)}
{holding_prompt}

请严格按照以下格式返回分析文本（不要返回JSON，直接返回文本；每段标题和顺序必须一致）：

### 趋势判断
[结合均线、量能、波动率分析当前走势格局，要引用具体数字如MA5价格、涨跌幅等]

### 资金态度
[分析主力资金流向，引用具体金额如"主力净流入XX亿"，判断资金态度]

### 热度评估
[分析市场关注度、概念热度、排名等]

### 操作建议
[如有持仓，给出详细的持仓操作建议，引用成本价、现价、盈亏比例、支撑压力位；如无持仓，给出买入/观望建议]

### 风险提示
[列出2-3个具体风险点，要具体不要泛泛而谈]"""

        resp = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3},
            timeout=30,
        )
        content = resp.json()["choices"][0]["message"]["content"].strip()
        # 清理可能的代码块标记
        if "```" in content:
            content = content.split("```")[1] if content.startswith("```") else content
            content = content.split("```")[0]
        return _merge_ai_analysis_with_snapshot(
            {"conclusion": content.strip(), "source": "deepseek"},
            code,
            name,
            market,
            technical,
            holding=holding,
            trade_date=trade_date,
            analysis_snapshot=analysis_snapshot,
        )
    except Exception as e:
        return _build_snapshot_ai_analysis(
            code,
            name,
            market,
            technical,
            holding=holding,
            trade_date=trade_date,
            analysis_snapshot=analysis_snapshot,
            fallback_reason=f"AI 分析请求失败，已切换为快照回退结果：{str(e)}",
        )


# ═══════════════════════════════════════════
# 复盘数据 API
# ═══════════════════════════════════════════

@router.get("/hot-data/daily-review")
def daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """每日复盘数据"""
    try:
        rows = _read_sql("SELECT * FROM st_daily_review WHERE review_date = :d", {"d": review_date})
        if not rows:
            fb = _fallback_date("st_daily_review", "review_date", review_date)
            if fb != review_date:
                rows = _read_sql("SELECT * FROM st_daily_review WHERE review_date = :d", {"d": fb})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": review_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": review_date, "data": [], "total": 0, "error": str(e)}


def _review_date_text(value: object, default: str) -> str:
    """Return a stable YYYY-MM-DD response value for DATE-like database values."""
    if value is None:
        return default
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return str(isoformat())[:10]
        except (TypeError, ValueError):
            pass
    return str(value)[:10] or default


def _is_missing_review_table_error(exc: BaseException) -> bool:
    """Distinguish an undeployed digest table from operational DB failures."""
    original = getattr(exc, "orig", None)
    args = getattr(original or exc, "args", ())
    if args and args[0] == 1146:  # MySQL ER_NO_SUCH_TABLE
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in ("doesn't exist", "does not exist", "no such table", "undefined table")
    ) and "st_quant_review_digest" in message


def _quant_review_lookup(review_date: str, adjust_type: int = 0):
    """Atomically resolve an exact digest or the latest earlier ready digest.

    An exact blocked row is intentionally returned as-is.  This keeps a failed
    target-date quality gate from being hidden by older prose.
    """
    rows = _read_sql(
        """
        SELECT *
        FROM st_quant_review_digest
        WHERE adjust_type = :adjust_type
          AND (
              review_date = :d
              OR (review_date < :d AND publish_status = 'ready')
          )
        ORDER BY (review_date = :d) DESC, review_date DESC
        LIMIT 1
        """,
        {"d": review_date, "adjust_type": adjust_type},
    )
    if not rows:
        return None, False
    row = rows[0]
    resolved_date = _review_date_text(row.get("review_date"), review_date)
    return row, resolved_date != review_date


@router.get("/hot-data/daily-review/quant")
def quant_daily_review(
    review_date: str = Query(default_factory=lambda: date.today().isoformat()),
    adjust_type: int = Query(default=0),
):
    """Quality-gated compact quantitative review, with explicit fallback state."""
    try:
        row, fallback = _quant_review_lookup(review_date, adjust_type)
        if row is None:
            return {
                "date": review_date,
                "requested_date": review_date,
                "fallback": False,
                "data": [],
                "total": 0,
            }
        resolved_date = _review_date_text(row.get("review_date"), review_date)
        return {
            "date": resolved_date,
            "requested_date": review_date,
            "fallback": bool(fallback),
            "data": [row],
            "total": 1,
        }
    except Exception as e:
        return {
            "date": review_date,
            "requested_date": review_date,
            "fallback": False,
            "data": [],
            "total": 0,
            "unavailable": _is_missing_review_table_error(e),
            "error": str(e),
        }


@router.get("/hot-data/research-radar")
def research_radar(trade_date: str = Query(default=None)):
    """研报/博主/财报趋势雷达，用于网站、早报和晚报统一展示。"""
    try:
        from biz.research_radar.radar import build_research_radar

        return build_research_radar(get_engine(), trade_date)
    except Exception as e:
        return {
            "trade_date": trade_date or date.today().isoformat(),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_signals": [],
            "report_sources": [],
            "themes": [],
            "stock_pool": [],
            "error": str(e),
        }


@router.get("/hot-data/premarket-theme-forecast")
def premarket_theme_forecast(session_date: str = Query(default=None)):
    """读取已冻结的09:08盘前主题预判，不混用盘中或收盘后结论。"""
    try:
        from biz.premarket.theme_forecast import load_premarket_theme_forecast

        return load_premarket_theme_forecast(
            get_engine(),
            session_date,
            allow_fallback=True,
        )
    except Exception as e:
        return {
            "requested_date": session_date or date.today().isoformat(),
            "session_date": session_date or date.today().isoformat(),
            "stage": "PREMARKET_0908",
            "fallback": False,
            "themes": [],
            "stock_candidates": [],
            "total": 0,
            "error": str(e),
        }


@router.get("/hot-data/daily-review-dates")
def daily_review_dates():
    """复盘数据可用日期列表"""
    dates: set[str] = set()
    errors: list[str] = []
    for table_name in ("st_quant_review_digest", "st_daily_review"):
        try:
            rows = _read_sql(
                f"SELECT DISTINCT review_date AS d FROM {table_name} ORDER BY review_date DESC"
            )
            dates.update(
                _review_date_text(row.get("d"), "")
                for row in rows
                if row.get("d") is not None
            )
        except Exception as exc:
            errors.append(f"{table_name}: {exc}")

    result = {"dates": sorted((item for item in dates if item), reverse=True)}
    if errors:
        result["warnings"] = errors
    return result


@router.post("/hot-data/daily-review/generate")
def generate_daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """生成复盘数据"""
    try:
        import subprocess
        import sys
        root = _Path(__file__).resolve().parents[3]
        cmd = [sys.executable, "-m", "biz.review.generate", review_date]
        child_env = build_child_env(root)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=str(root), env=child_env)
        return {"date": review_date, "status": "success" if r.returncode == 0 else "failed",
                "output": (r.stdout or "")[-500:] + (r.stderr or "")[-500:]}
    except Exception as e:
        return {"date": review_date, "status": "error", "output": str(e)}


@router.get("/hot-data/daily-review/print")
def print_daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """复盘数据打印页 — 含图表"""
    try:
        rows = _read_sql("SELECT * FROM st_daily_review WHERE review_date = :d", {"d": review_date})
        if not rows:
            return HTMLResponse("<h2>暂无数据</h2>")
        r = rows[0]

        def jl(key):
            v = r.get(key)
            if v is None: return []
            if isinstance(v, list): return v
            if isinstance(v, dict): return [v]
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except (TypeError, json.JSONDecodeError) as exc:
                    logger.debug("Failed to parse review sector JSON field %s: %s", key, exc)
                    return []
            return []

        hot = jl("hot_sectors")
        cold = jl("cold_sectors")
        vol_up = jl("volume_up_sectors")
        vol_down = jl("volume_down_sectors")
        idx_a = jl("index_analysis")

        heat_pct = float(r.get("market_heat") or 0)
        total_amt = float(r.get("total_amount") or 0)
        idx_name = str(r.get("index_name") or "中证1000")
        idx_price = float(r.get("index_price") or 0)
        idx_chg = float(r.get("index_change_pct") or 0)
        sideline = float(r.get("sideline_ratio") or 0)

        heat_note = str(r.get("market_heat_note") or "")
        up_count = 0; down_count = 0
        import re as _re
        m = _re.search(r"上涨([\d.]+)", heat_note)
        if m: up_count = int(float(m.group(1)))
        m = _re.search(r"下跌([\d.]+)", heat_note)
        if m: down_count = int(float(m.group(1)))

        charts_html = ""
        if _CHARTS_AVAILABLE:
            try:
                b64 = chart_market_heat(heat_pct, up_count or 500, down_count or 4000, total_amt,
                                        idx_name, idx_price, idx_chg, sideline)
                charts_html += f'<img src="{b64}" style="width:100%;max-width:900px;margin:8px 0">'
            except Exception as e:
                charts_html += f"<!-- market chart err: {e} -->"

            try:
                combined = hot[:8] + cold[:8]
                combined.sort(key=lambda x: float(x.get("change_pct", 0) or 0))
                b64 = chart_sector_bars(hot[:8], cold[:8])
                charts_html += f'<img src="{b64}" style="width:100%;max-width:800px;margin:8px 0">'
            except Exception as e:
                charts_html += f"<!-- sector chart err: {e} -->"

            ia = idx_a[0] if idx_a else {}
            ma20 = float(ia.get("ma20", 0) or 0)
            level = str(ia.get("level") or "均线附近")
            if idx_price > 0 and ma20 > 0:
                try:
                    b64 = chart_index_position(idx_price, ma20, level, idx_name)
                    charts_html += f'<img src="{b64}" style="width:100%;max-width:800px;margin:8px 0">'
                except Exception as e:
                    charts_html += f"<!-- index chart err: {e} -->"

            if vol_up or vol_down:
                try:
                    b64 = chart_volume_tags(vol_up, vol_down)
                    charts_html += f'<img src="{b64}" style="width:100%;max-width:800px;margin:8px 0">'
                except Exception as e:
                    charts_html += f"<!-- volume chart err: {e} -->"

        amt = f"{total_amt/1e8:.0f}亿"
        heat = f"{heat_pct:.1f}%"

        def tag_list(items):
            if not items: return ""
            return " ".join([f"<span class='tag'>{s.get('name','-') if isinstance(s,dict) else s}</span>" for s in items])

        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>复盘数据 | {review_date}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:#1a1a2e;padding:16px;color:#e0e0e0}}
.page{{max-width:960px;margin:0 auto;background:#16213e;padding:28px 36px;border-radius:10px;box-shadow:0 2px 16px rgba(0,0,0,.4)}}
h1{{font-size:22px;color:#e0e0e0;margin-bottom:4px;border-bottom:2px solid #1a73e8;padding-bottom:10px}}
.sub{{font-size:12px;color:#888;margin-bottom:20px}}
h2{{font-size:16px;color:#e0e0e0;margin:22px 0 10px;padding-left:8px;border-left:3px solid #1a73e8}}
.cards{{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:10px}}
.card{{flex:1;min-width:100px;background:#1a1a2e;padding:12px 14px;border-radius:6px;text-align:center;border:1px solid #2a2a4e}}
.card .label{{font-size:11px;color:#888;margin-bottom:4px}}
.card .val{{font-size:17px;font-weight:700}}
.val.blue{{color:#1a73e8}}.val.red{{color:#e53935}}.val.green{{color:#43a047}}.val.orange{{color:#ff9800}}
.note{{font-size:13px;color:#aaa;line-height:1.6;margin:8px 0 18px}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:8px 0 14px}}
th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #2a2a4e}}
th{{color:#888;font-weight:500;font-size:12px}}
.up{{color:#e53935;font-weight:600}}.down{{color:#43a047;font-weight:600}}
.tag{{display:inline-block;background:#1a1a2e;color:#1a73e8;padding:3px 8px;border-radius:3px;margin:2px;font-size:12px;border:1px solid #2a2a4e}}
.tech{{background:#1a1a2e;padding:10px 14px;border-radius:6px;margin:6px 0;font-size:13px;color:#aaa;border:1px solid #2a2a4e}}
.summary{{background:#1a1a2e;border-left:3px solid #1a73e8;padding:12px 16px;border-radius:4px;margin:12px 0;font-size:13px;color:#aaa;line-height:1.8;white-space:pre-wrap}}
.disclaimer{{font-size:11px;color:#666;margin-top:18px;text-align:center}}
.img-chart{{display:block;width:100%%;max-width:900px;margin:10px auto;border-radius:6px}}
@media print{{
  body{{background:#fff;padding:0;color:#222}}
  .page{{background:#fff;box-shadow:none;border-radius:0;padding:20px 30px;border:1px solid #ddd}}
  .card{{background:#f8f9fc;border:1px solid #e0e0e0}}
  .card .label{{color:#666}}.card .val.blue{{color:#1a73e8}}.card .val.red{{color:#e53935}}.card .val.green{{color:#43a047}}.card .val.orange{{color:#e65100}}
  h1,h2,.note,.tech,.summary{{color:#222}}
  .note{{color:#555}}.tag{{background:#e8f0fe}}.tech,.summary{{color:#555;background:#f8f9fc;border-color:#e0e0e0}}
  th,td{{border-color:#e0e0e0}}th{{color:#666}}
  .disclaimer{{color:#999}}
  .img-chart{{filter:invert(0)}}
  .img-chart img{{background:#fff}}
}}
</style>
</head>
<body>
<div class="page">
<h1>📋 复盘数据 | {review_date}</h1>
<div class="sub">ProBigA 智能生成 · 复盘参考</div>

{charts_html}

<h2>📊 市场总览</h2>
<div class="cards">
<div class="card"><div class="label">市场热度</div><div class="val blue">{heat}</div></div>
<div class="card"><div class="label">成交额</div><div class="val orange">{amt}</div></div>
<div class="card"><div class="label">{idx_name}</div><div class="val">{idx_price}</div></div>
<div class="card"><div class="label">涨跌幅</div><div class="val {"red" if idx_chg>=0 else "green"}">{idx_chg:+.2f}%</div></div>
<div class="card"><div class="label">量能</div><div class="val">{r.get("total_amount_change","-")}</div></div>
<div class="card"><div class="label">观望资金</div><div class="val">{sideline:.1f}%</div></div>
</div>
<div class="note">{heat_note}</div>

<h2>🔥 板块热度</h2>
<table><thead><tr><th style="width:50%">热度上升板块</th><th style="width:20%">涨跌幅</th><th style="width:30%">热度下降板块</th><th style="width:20%">涨跌幅</th></tr></thead><tbody>
"""
        max_len = max(len(hot), len(cold))
        for i in range(max_len):
            h = hot[i] if i < len(hot) else None
            cold_item = cold[i] if i < len(cold) else None
            html += "<tr>"
            if h and isinstance(h, dict):
                chg = h.get("change_pct")
                html += f"<td>{h.get('name','-')}</td><td class=\"{'up' if (chg or 0) >= 0 else 'down'}\">{chg:+.2f}%</td>"
            else:
                html += "<td></td><td></td>"
            if cold_item and isinstance(cold_item, dict):
                chg = cold_item.get("change_pct")
                html += f"<td>{cold_item.get('name','-')}</td><td class=\"{'up' if (chg or 0) >= 0 else 'down'}\">{chg:+.2f}%</td>"
            else:
                html += "<td></td><td></td>"
            html += "</tr>"
        html += "</tbody></table>"

        if vol_up or vol_down:
            html += "<h2>📈 量能变化</h2>"
            if vol_up:
                html += f"<div style='margin-bottom:6px'><strong style='color:#ff9800'>放量板块:</strong> {tag_list(vol_up)}</div>"
            if vol_down:
                html += f"<div><strong style='color:#1a73e8'>缩量板块:</strong> {tag_list(vol_down)}</div>"

        if idx_a:
            html += "<h2>📐 指数技术分析</h2>"
            for ia in idx_a:
                if isinstance(ia, dict):
                    html += f"<div class='tech'><b>{ia.get('name','')}</b> ({ia.get('price','')}) — {ia.get('note','')} | 均线: {ia.get('ma20','-')}</div>"

        html += f"<h2>📝 综合结论</h2>"
        html += f"<div class='summary'>{r.get('summary','暂无')}</div>"
        html += f"<div class='disclaimer'>{r.get('disclaimer','⚠️ 本建议仅供参考，不构成投资建议')}</div>"
        html += """</div>
</body></html>"""

        return Response(content=html.encode("utf-8"), media_type="text/html; charset=utf-8",
                        headers={"Content-Disposition": f"attachment; filename=review_{review_date}.html"})
    except Exception as e:
        return Response(f"<h2>生成失败: {e}</h2>".encode("utf-8"),
                        media_type="text/html; charset=utf-8")


@router.get("/hot-data/daily-review/export")
def export_daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """导出复盘文本，量化门禁结果优先且 blocked 时禁止旧版回退。"""
    try:
        try:
            quant_row, quant_fallback = _quant_review_lookup(review_date, 0)
        except Exception as exc:
            # A deployment without the new table can still export historical
            # legacy reviews. Operational failures must not silently substitute
            # stale legacy prose for the quality-gated result.
            if _is_missing_review_table_error(exc):
                logger.debug("quant review export table unavailable: %s", exc)
                quant_row, quant_fallback = None, False
            else:
                return {
                    "date": review_date,
                    "publish_status": "error",
                    "error": f"量化复盘查询失败: {exc}",
                }

        if quant_row is not None:
            resolved_date = _review_date_text(quant_row.get("review_date"), review_date)
            status = str(quant_row.get("publish_status") or "").lower()
            quality = quant_row.get("quality_json")
            if isinstance(quality, str):
                try:
                    quality = json.loads(quality)
                except (TypeError, json.JSONDecodeError):
                    quality = {"errors": [quality] if quality else []}
            if not isinstance(quality, dict):
                quality = {}
            if status != "ready":
                return {
                    "date": resolved_date,
                    "requested_date": review_date,
                    "fallback": False,
                    "publish_status": status or "blocked",
                    "error": "量化复盘未通过质量门禁",
                    "quality": quality,
                }
            return {
                "date": resolved_date,
                "requested_date": review_date,
                "fallback": bool(quant_fallback),
                "publish_status": "ready",
                "text": str(quant_row.get("compact_review") or ""),
            }

        # The target date has no quant digest (and no earlier ready digest), so
        # preserve access to historical professional/basic review exports.
        pro_rows = _read_sql("SELECT pro_review FROM st_daily_review_pro WHERE review_date = :d", {"d": review_date})
        if pro_rows and pro_rows[0].get("pro_review"):
            return {"date": review_date, "text": pro_rows[0]["pro_review"]}

        rows = _read_sql("SELECT * FROM st_daily_review WHERE review_date = :d", {"d": review_date})
        if not rows:
            return {"error": "无数据"}
        r = rows[0]

        def jl(key):
            v = r.get(key)
            if v is None:
                return []
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
            if isinstance(v, str):
                for parser in [json.loads, __import__('ast').literal_eval]:
                    try:
                        parsed = parser(v)
                        if isinstance(parsed, list):
                            return parsed
                        if isinstance(parsed, dict):
                            return [parsed]
                    except Exception:
                        continue
            return []

        safe_get = lambda k, d='-': str(r.get(k, d) or d)

        text_lines = [f"# 复盘数据 | {review_date}", ""]
        text_lines.append("## 市场总览")
        text_lines.append(f"- 市场热度: {safe_get('market_heat')}%")
        text_lines.append(f"- 热度变化: {safe_get('market_heat_note')}")
        text_lines.append(f"- 情绪周期: {safe_get('sentiment_cycle')} / {safe_get('sentiment_cycle_desc')}")
        text_lines.append(
            f"- 接力环境: 涨停{safe_get('limit_up_count')}家，跌停{safe_get('limit_down_count')}家，"
            f"炸板{safe_get('broken_board_count')}家，炸板率{safe_get('broken_rate')}%，最高{safe_get('max_boards')}板"
        )
        text_lines.append(f"- 成交额: {float(r.get('total_amount', 0)) / 1e8:.0f}亿")
        text_lines.append(f"- 量能变化: {safe_get('total_amount_change')}")
        text_lines.append(f"- 主要指数: {safe_get('index_name')} {safe_get('index_price')} ({safe_get('index_change_pct')}%)")
        text_lines.append(f"- 观望资金: {safe_get('sideline_ratio')}%")
        text_lines.append("")

        for label, key in [("热度上升板块", "hot_sectors"), ("热度下降板块", "cold_sectors")]:
            items = jl(key)
            if items:
                text_lines.append(f"## {label}")
                for s in items:
                    if isinstance(s, dict):
                        text_lines.append(f"- {s.get('name', s)} (涨跌幅: {s.get('change_pct', '-')}%)")
                    else:
                        text_lines.append(f"- {s}")
                text_lines.append("")

        for label, key in [("放量板块", "volume_up_sectors"), ("缩量板块", "volume_down_sectors")]:
            items = jl(key)
            if items:
                text_lines.append(f"## {label}")
                for s in items:
                    text_lines.append(f"- {s.get('name', s) if isinstance(s, dict) else s}")
                text_lines.append("")

        idx_a = jl("index_analysis")
        if idx_a:
            text_lines.append("## 指数技术分析")
            for ia in idx_a:
                if isinstance(ia, dict):
                    text_lines.append(f"- {ia.get('name','')}: {ia.get('note','')}")
                else:
                    text_lines.append(f"- {ia}")
            text_lines.append("")

        text_lines.append("## 综合结论")
        text_lines.append(r.get("summary", ""))
        text_lines.append("")
        text_lines.append(f"> {r.get('disclaimer', '')}")

        return {"date": review_date, "text": "\n".join(text_lines)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/hot-data/daily-review/pro")
def pro_daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """专业复盘数据"""
    try:
        rows = _read_sql("SELECT * FROM st_daily_review_pro WHERE review_date = :d", {"d": review_date})
        if not rows:
            fb = _fallback_date("st_daily_review_pro", "review_date", review_date)
            if fb != review_date:
                rows = _read_sql("SELECT * FROM st_daily_review_pro WHERE review_date = :d", {"d": fb})
                return {"date": fb, "fallback": True, "data": rows, "total": len(rows)}
        return {"date": review_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": review_date, "data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 自选股持仓 API
# ═══════════════════════════════════════════

from pydantic import BaseModel


_PORTFOLIO_STOCK_CODE_RE = re.compile(r"^\d{6}$")


def _require_portfolio_stock_code(value: object) -> str:
    """Validate stock codes received from portfolio mutation endpoints."""
    code = str(value or "").strip()
    if not _PORTFOLIO_STOCK_CODE_RE.fullmatch(code):
        raise ValueError("stock_code must be exactly 6 digits")
    return code


def _safe_portfolio_stock_codes(values) -> list[str]:
    """Normalize trusted legacy rows and discard malformed query inputs."""
    clean: list[str] = []
    seen: set[str] = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        code = raw.zfill(6)
        if not _PORTFOLIO_STOCK_CODE_RE.fullmatch(code) or code in seen:
            continue
        seen.add(code)
        clean.append(code)
    return clean


def _portfolio_stock_code_query(
    values,
    *,
    prefix: str = "portfolio_code",
) -> tuple[list[str], str, dict[str, str]]:
    clean = _safe_portfolio_stock_codes(values)
    placeholders = ", ".join(f":{prefix}_{idx}" for idx, _ in enumerate(clean))
    params = {f"{prefix}_{idx}": code for idx, code in enumerate(clean)}
    return clean, placeholders, params


class PortfolioAdd(BaseModel):
    stock_code: str
    cost_price: float = 0
    shares: int = 0
    notes: str = ""
    is_today_buy: bool = False

class PortfolioTransact(BaseModel):
    stock_code: str = ""
    trans_type: str = "buy"
    price: float = 0
    shares: int = 0


def _ensure_portfolio_trans_log_table():
    try:
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_portfolio_trans_log` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `stock_code` VARCHAR(16) NOT NULL,
              `trans_type` VARCHAR(8) NOT NULL,
              `price` DECIMAL(12,4) NOT NULL DEFAULT 0,
              `shares` INT NOT NULL DEFAULT 0,
              `trans_date` DATE NOT NULL,
              `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              KEY `idx_code_date` (`stock_code`, `trans_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _portfolio_ensure_column("st_portfolio_trans_log", "source", "ALTER TABLE `st_portfolio_trans_log` ADD COLUMN `source` VARCHAR(16) DEFAULT 'trade' COMMENT '来源：trade/position_add' AFTER `shares`")
    except Exception as exc:
        _record_fallback('_ensure_portfolio_trans_log_table:4273', exc)


def _portfolio_ensure_column(table: str, column: str, ddl: str) -> None:
    try:
        rows = _read_sql("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :t AND column_name = :c
        """, {"t": table, "c": column})
        if not rows or int(rows[0].get("cnt") or 0) == 0:
            _exec_sql(ddl)
    except Exception as exc:
        _record_fallback('_portfolio_ensure_column:4286', exc)


def _ensure_portfolio_position_columns() -> None:
    _portfolio_ensure_column("st_user_portfolio", "position_source", "ALTER TABLE `st_user_portfolio` ADD COLUMN `position_source` VARCHAR(16) DEFAULT 'manual' COMMENT '持仓来源：manual/today_buy' AFTER `shares`")
    _portfolio_ensure_column("st_user_portfolio", "position_date", "ALTER TABLE `st_user_portfolio` ADD COLUMN `position_date` DATE DEFAULT NULL COMMENT '持仓来源日期' AFTER `position_source`")
    _portfolio_ensure_cost_price_precision()


def _portfolio_ensure_cost_price_precision() -> None:
    try:
        rows = _read_sql("""
            SELECT numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'st_user_portfolio' AND column_name = 'cost_price'
        """)
        if not rows:
            return
        precision = int(rows[0].get("numeric_precision") or 0)
        scale = int(rows[0].get("numeric_scale") or 0)
        if precision < 12 or scale < 4:
            _exec_sql("ALTER TABLE `st_user_portfolio` MODIFY COLUMN `cost_price` DECIMAL(12,4) NOT NULL DEFAULT 0 COMMENT '成本价'")
    except Exception as exc:
        _record_fallback('_portfolio_ensure_cost_price_precision:4309', exc)


def _portfolio_log_trans(code: str, trans_type: str, price: float, shares: int, source: str = "trade"):
    _ensure_portfolio_trans_log_table()
    _exec_sql("""
        INSERT INTO st_portfolio_trans_log (stock_code, trans_type, price, shares, source, trans_date, created_at)
        VALUES (:c, :t, :p, :s, :src, CURDATE(), NOW())
    """, {"c": code, "t": trans_type, "p": price, "s": shares, "src": source})


def _watchlist_write_flow(code: str, short_name: str, trans_type: str, price: float, shares: int):
    """自选股操作同步写入操作流水表 st_trade_flow"""
    try:
        from server.api.routers.portfolio_math import portfolio_trade_fee as _fee
        amount = round(price * shares, 2)
        fee = round(_fee(trans_type, price, shares), 2)
        flow_type = "watch_buy" if trans_type == "buy" else "watch_sell"
        _exec_sql("""
            INSERT INTO st_trade_flow
            (stock_code, short_name, flow_type, source, strategy_type, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, :ft, 'watchlist', '', :tt,
                    :price, :shares, :amount, :fee, '自选股操作', 0, CURDATE(), DATE_FORMAT(NOW(), '%H:%i'))
        """, {"code": code, "name": short_name or "", "ft": flow_type, "tt": trans_type,
              "price": price, "shares": shares, "amount": amount, "fee": fee})
    except Exception as exc:
        _record_fallback('_watchlist_write_flow:4336', exc)


def _portfolio_cost_profit(shares: int, cur_price: float, cost_price: float) -> float | None:
    return portfolio_cost_profit(shares, cur_price, cost_price)


def _portfolio_quote_trade_date(row: dict | None = None) -> str:
    if _portfolio_market_mode() == "intraday":
        return date.today().isoformat()
    return _portfolio_close_trade_date()


def _portfolio_trade_fee(trans_type: str, price: float, shares: int) -> float:
    return portfolio_trade_fee(trans_type, price, shares)


def _portfolio_calc_next_position(
    trans_type: str,
    old_cost: float,
    old_shares: int,
    price: float,
    shares: int,
) -> dict:
    return portfolio_calc_next_position(trans_type, old_cost, old_shares, price, shares)


def _portfolio_apply_quote(row: dict, quote: dict | None) -> None:
    if not quote or quote.get("price") is None:
        return
    row["cur_price"] = quote.get("price")
    row["live_price"] = quote.get("price")
    if quote.get("change") is not None:
        row["price_change"] = quote.get("change")
    if quote.get("change_pct") is not None:
        row["change_pct"] = quote.get("change_pct")
    if quote.get("short_name"):
        row["current_name"] = quote.get("short_name")
    if quote.get("snapshot_at"):
        row["quote_snapshot_at"] = quote.get("snapshot_at")
        row["quote_trade_date"] = str(quote.get("snapshot_at") or "")[:10]
    if quote.get("source"):
        row["quote_source"] = quote.get("source")
    row["quote_status"] = quote.get("quote_status") or "fresh"
    row["quote_age_seconds"] = quote.get("quote_age_seconds")
    if quote.get("amount") is not None:
        row["quote_amount"] = quote.get("amount")
    if quote.get("volume") is not None:
        row["quote_volume"] = quote.get("volume")


def _portfolio_apply_kline_quote(row: dict, kline: dict | None, *, status: str = "closed") -> None:
    if not kline or kline.get("close") is None:
        return
    row["cur_price"] = kline.get("close")
    row["change_pct"] = kline.get("change_pct")
    if kline.get("short_name"):
        row["current_name"] = kline.get("short_name")
    row["quote_source"] = "daily_kline"
    row["quote_status"] = status
    row["quote_trade_date"] = str(kline.get("trade_date") or "")[:10]
    row["quote_snapshot_at"] = ""
    row["quote_age_seconds"] = None


def _portfolio_rebase_quote_change(row: dict, kline: dict | None) -> None:
    """Recompute intraday change from live price and the latest daily close."""
    if not row or not kline or row.get("cur_price") is None:
        return
    price = _portfolio_num(row.get("cur_price"))
    if price <= 0:
        return
    quote_trade_date = str(row.get("quote_trade_date") or row.get("quote_snapshot_at") or "")[:10]
    kline_trade_date = str(kline.get("trade_date") or "")[:10]
    prev_close = 0.0
    if quote_trade_date and kline_trade_date and quote_trade_date > kline_trade_date:
        prev_close = _portfolio_num(kline.get("close"))
    elif quote_trade_date and kline_trade_date and quote_trade_date == kline_trade_date:
        prev_close = _portfolio_num(kline.get("pre_close"))
    elif str(row.get("quote_source") or "") == "daily_kline":
        prev_close = _portfolio_num(kline.get("pre_close"))
    elif kline_trade_date:
        prev_close = _portfolio_num(kline.get("close"))
    if prev_close <= 0:
        return
    change = price - prev_close
    row["quote_prev_close"] = round(prev_close, 4)
    row["price_change"] = round(change, 2)
    row["change_pct"] = round(change / prev_close * 100, 2)


def _portfolio_flow_attitude(flow_value, amount_value=None) -> dict:
    value = float(flow_value or 0)
    amount = float(amount_value or 0)
    ratio = (value / amount * 100) if amount > 0 else None
    score = ratio if ratio is not None else (value / 1_000_000)
    if score >= 8:
        label, level = "强进", "strong_in"
    elif score >= 3:
        label, level = "流入", "in"
    elif score <= -8:
        label, level = "强出", "strong_out"
    elif score <= -3:
        label, level = "流出", "out"
    else:
        label, level = "中性", "neutral"
    return {
        "label": label,
        "level": level,
        "ratio": round(ratio, 2) if ratio is not None else None,
    }


def _portfolio_num(value, default=0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _portfolio_daily_flow_attitude(main_net_inflow, amount_value=None) -> dict:
    value = _portfolio_num(main_net_inflow)
    amount = _portfolio_num(amount_value)
    ratio = (value / amount * 100) if amount > 0 else None
    if ratio is not None:
        return _portfolio_flow_attitude(value, amount)
    abs_value = abs(value)
    if value >= 100_000_000:
        label, level = "强进", "strong_in"
    elif value >= 20_000_000:
        label, level = "流入", "in"
    elif value <= -100_000_000:
        label, level = "强出", "strong_out"
    elif value <= -20_000_000:
        label, level = "流出", "out"
    else:
        label, level = "中性", "neutral"
    return {
        "label": label,
        "level": level,
        "ratio": round(value / 100_000_000, 2) if abs_value else 0,
    }


def _portfolio_time_age_seconds(value) -> int | None:
    dt = None
    if isinstance(value, datetime):
        dt = value.replace(tzinfo=None)
    elif value:
        text_value = str(value).strip()
        for fmt, width in (
            ("%Y-%m-%d %H:%M:%S", 19),
            ("%Y-%m-%d %H:%M", 16),
            ("%Y-%m-%d", 10),
        ):
            try:
                dt = datetime.strptime(text_value[:width], fmt)
                break
            except Exception:
                continue
    if not dt:
        return None
    return max(0, int((datetime.now() - dt).total_seconds()))


def _portfolio_build_watch_analysis(row: dict) -> dict:
    """Compact intraday watch analysis for the portfolio table."""
    change_pct = _portfolio_num(row.get("change_pct"))
    price = _portfolio_num(row.get("cur_price"))
    shares = int(row.get("shares") or 0)
    is_holding = bool(row.get("is_holding") or shares > 0)
    reported_main_flow = _portfolio_num(row.get("main_net_inflow"))
    quote_amount = _portfolio_num(row.get("quote_amount") or row.get("amount"))
    minute_level = str(row.get("flow_attitude") or "")
    minute_label = str(row.get("flow_attitude_label") or "")
    flow_status = str(row.get("flow_status") or "")
    flow_usable = flow_status in {"fresh", "closed"} and row.get("main_net_inflow") is not None
    main_flow = reported_main_flow if flow_usable else 0.0
    if flow_status == "fresh" and minute_level and minute_label:
        flow_att = {"level": minute_level, "label": minute_label, "ratio": row.get("flow_attitude_ratio")}
        flow_basis = "minute_5m_fresh"
    elif flow_status == "closed" and flow_usable:
        flow_att = (
            {"level": minute_level, "label": minute_label, "ratio": row.get("flow_attitude_ratio")}
            if minute_level and minute_label
            else _portfolio_daily_flow_attitude(main_flow, quote_amount)
        )
        flow_basis = str(row.get("flow_attitude_basis") or "daily_close")
    else:
        flow_att = {"level": "neutral", "label": "暂无", "ratio": None}
        flow_basis = "unavailable"
    signal_flow = (
        _portfolio_num(row.get("flow_5m"))
        if flow_basis == "minute_5m_fresh" and row.get("flow_5m") is not None
        else main_flow
    )
    flow_level = str(flow_att.get("level") or "neutral")
    flow_label = str(flow_att.get("label") or "中性")
    macro_risk_triggered = bool(row.get("macro_risk_triggered"))
    macro_risk_reason = str(row.get("macro_risk_reason") or "")

    if price <= 0:
        trend = "缺价"
    elif change_pct >= 5:
        trend = "强势"
    elif change_pct >= 1:
        trend = "偏强"
    elif change_pct <= -5:
        trend = "弱势"
    elif change_pct <= -1:
        trend = "偏弱"
    else:
        trend = "震荡"

    heat_score = 50
    if abs(change_pct) >= 7:
        heat_score += 20
    elif abs(change_pct) >= 3:
        heat_score += 10
    if abs(signal_flow) >= 100_000_000:
        heat_score += 20
    elif abs(signal_flow) >= 20_000_000:
        heat_score += 10
    if quote_amount >= 1_000_000_000:
        heat_score += 10
    heat_score = max(0, min(100, heat_score))
    heat = "高" if heat_score >= 75 else "中" if heat_score >= 55 else "低"

    if price <= 0:
        advice = "等价"
        decision_rule = "实时价格缺失，先不做买卖判断。"
    elif flow_level in {"strong_out", "out"} and change_pct <= -2:
        advice = "控仓" if is_holding else "观望"
        decision_rule = "资金外流且跌幅超过 2%，优先控制风险。"
    elif flow_level in {"strong_in", "in"} and change_pct >= 1:
        advice = "持有" if is_holding else "关注"
        decision_rule = "资金流入且涨幅超过 1%，说明短线承接偏强。"
    elif change_pct >= 8:
        advice = "防追高" if not is_holding else "盯卖点"
        decision_rule = "涨幅超过 8%，追高风险上升，持仓优先盯卖点。"
    elif is_holding and macro_risk_triggered:
        advice = "先跑"
        decision_rule = "事件/板块风险命中当前持仓，优先降低暴露。"
    elif is_holding and change_pct <= -4:
        advice = "看止损"
        decision_rule = "持仓股跌幅超过 4%，需要复核止损与承接。"
    elif is_holding:
        advice = "持有"
        decision_rule = "仍有持仓且未触发强风险条件，默认继续观察持有。"
    else:
        advice = "观察"
        decision_rule = "未持仓且没有形成明确资金/价格共振，先观察。"

    risk_parts: list[str] = []
    if price <= 0:
        risk_parts.append("实时价缺失")
    if row.get("flow_trade_date") and row.get("quote_trade_date") and str(row.get("flow_trade_date"))[:10] < str(row.get("quote_trade_date"))[:10]:
        risk_parts.append("资金滞后")
    if flow_status == "stale":
        risk_parts.append("当日资金滞后")
    elif flow_status == "missing":
        risk_parts.append("当日资金缺失")
    if flow_level in {"strong_out", "out"}:
        risk_parts.append("资金外流")
    if macro_risk_triggered:
        risk_parts.insert(0, "事件风险命中")
    if change_pct >= 9.5:
        risk_parts.append("接近涨停")
    if change_pct <= -7:
        risk_parts.append("跌幅较大")
    if is_holding and row.get("profit_pct") is not None and _portfolio_num(row.get("profit_pct")) <= -5:
        risk_parts.append("持仓亏损扩大")
    if not risk_parts:
        risk_parts.append("暂无明显")

    guard_level = "LOW"
    guard_action = "正常持有" if is_holding else "未持仓观察"
    guard_reason = "暂无明显回撤压力"
    profit_pct = _portfolio_num(row.get("profit_pct")) if row.get("profit_pct") is not None else None
    stop_loss_line = round(float(row.get("cost_price") or 0) * 0.95, 3) if is_holding and _portfolio_num(row.get("cost_price")) > 0 else None
    reduce_line = round(price * 0.97, 3) if is_holding and price > 0 else None

    if price <= 0:
        guard_level = "DATA"
        guard_action = "等行情"
        guard_reason = "实时价缺失，先不做交易判断"
    elif is_holding:
        if macro_risk_triggered:
            guard_level = "HIGH"
            guard_action = "事件先跑"
            guard_reason = macro_risk_reason or "黑天鹅/板块风险命中持仓，优先降仓防守"
        elif profit_pct is not None and (profit_pct <= -8 or (profit_pct <= -5 and (flow_level in {"strong_out", "out"} or change_pct <= -3))):
            guard_level = "HIGH"
            guard_action = "止损复核"
            guard_reason = "持仓亏损已扩大，且价格或资金未确认修复"
        elif change_pct <= -7 or (flow_level == "strong_out" and change_pct <= -4):
            guard_level = "HIGH"
            guard_action = "减仓防守"
            guard_reason = "盘中急跌叠加资金压力，优先控制回撤"
        elif profit_pct is not None and profit_pct >= 20 and change_pct <= -4:
            guard_level = "HIGH"
            guard_action = "止盈保护"
            guard_reason = "盈利较厚但出现明显回吐，先保护利润"
        elif profit_pct is not None and profit_pct >= 10 and flow_level in {"strong_out", "out"} and change_pct <= -2:
            guard_level = "MEDIUM"
            guard_action = "分批止盈"
            guard_reason = "已有利润且资金外流，避免盈利回撤"
        elif profit_pct is not None and profit_pct <= -3 and flow_level in {"strong_out", "out"}:
            guard_level = "MEDIUM"
            guard_action = "降仓观察"
            guard_reason = "小亏叠加资金外流，先降低暴露"
        elif change_pct <= -4:
            guard_level = "MEDIUM"
            guard_action = "看止损线"
            guard_reason = "跌幅偏大，观察是否跌破防守线"

    quote_status_for_label = str(row.get("quote_status") or "")
    if quote_status_for_label == "fresh":
        freshness = "实时"
    elif quote_status_for_label == "closed":
        freshness = "收盘"
    elif quote_status_for_label == "previous_close":
        freshness = "上一收盘"
    elif row.get("quote_snapshot_at") or row.get("live_price") is not None:
        freshness = "快照"
    else:
        freshness = "收盘"
    if flow_status == "fresh":
        freshness += "/分资"
    elif flow_status == "closed":
        freshness += "/收盘资金"
    elif flow_status == "stale":
        freshness += "/资金旧"
    else:
        freshness += "/资金缺"

    confidence = 50
    data_flags: list[str] = []
    if price > 0:
        confidence += 10
    else:
        confidence -= 25
        data_flags.append("实时价缺失")
    if flow_basis == "minute_5m_fresh":
        confidence += 16
    elif flow_status == "closed":
        confidence += 10
        data_flags.append("资金使用当日收盘口径")
    else:
        confidence -= 10
        data_flags.append("当日资金滞后或缺失")
    if row.get("quote_status") == "fresh":
        confidence += 8
    elif row.get("quote_status") == "closed":
        confidence += 6
    elif row.get("quote_status") == "previous_close":
        confidence -= 6
        data_flags.append("行情使用上一交易日收盘")
    elif row.get("quote_status") == "stale":
        confidence -= 8
        data_flags.append("行情可能滞后")
    if flow_status == "stale":
        confidence -= 10
    if macro_risk_triggered:
        confidence += 6
    if risk_parts != ["暂无明显"]:
        confidence -= min(12, len(risk_parts) * 3)
    confidence = int(max(15, min(92, confidence)))
    confidence_label = "高" if confidence >= 75 else "中" if confidence >= 55 else "低"
    if not data_flags:
        data_flags.append("行情/资金数据可用于盘中判断")

    flow_ratio_text = ""
    if flow_att.get("ratio") is not None:
        try:
            flow_ratio_text = f"，占成交额 {float(flow_att.get('ratio')):.1f}%"
        except Exception:
            flow_ratio_text = ""
    evidence_flow = signal_flow if flow_usable else None
    funds_source_label = (
        "近5分钟增量"
        if flow_basis == "minute_5m_fresh"
        else "当日收盘资金"
        if flow_status == "closed"
        else "当日资金不可用"
    )
    evidence = [
        {
            "label": "价格",
            "value": f"现价 {price:.2f}，涨跌 {change_pct:+.2f}%" if price > 0 else "实时价缺失",
            "tone": "good" if change_pct >= 1 else "bad" if change_pct <= -2 else "neutral",
            "explain": f"按涨跌幅分层，当前归类为“{trend}”。",
        },
        {
            "label": "资金",
            "value": (
                f"{flow_label} / {_fmt_analysis_money_cn(evidence_flow)}{flow_ratio_text}"
                if evidence_flow is not None
                else "暂无可用的当日资金数据"
            ),
            "tone": "good" if flow_level in {"strong_in", "in"} else "bad" if flow_level in {"strong_out", "out"} else "neutral",
            "explain": f"资金来源：{funds_source_label}。",
        },
        {
            "label": "热度",
            "value": f"{heat}（{heat_score}/100）",
            "tone": "good" if heat_score >= 75 else "neutral" if heat_score >= 55 else "muted",
            "explain": "由涨跌幅、主力净流、成交额共同加权，不等于买入信号。",
        },
        {
            "label": "持仓",
            "value": f"{shares} 股" if is_holding else "未持仓",
            "tone": "neutral",
            "explain": (
                f"成本 {float(row.get('cost_price') or 0):.2f}，浮盈 {profit_pct:+.2f}%。"
                if is_holding and profit_pct is not None
                else "未持仓时建议偏向观察/关注，不直接给仓位动作。"
            ),
        },
    ]
    if macro_risk_triggered:
        evidence.insert(0, {
            "label": "事件风险",
            "value": "命中",
            "tone": "bad",
            "explain": macro_risk_reason or "当前持仓匹配到事件/板块风险信号。",
        })

    next_checks = []
    if guard_reason:
        next_checks.append(f"复核回撤守门：{guard_reason}")
    if stop_loss_line:
        next_checks.append(f"若跌破止损线 {stop_loss_line}，优先复核减仓/止损。")
    if reduce_line:
        next_checks.append(f"若跌破减仓观察线 {reduce_line} 且资金继续外流，降低仓位。")
    if flow_level in {"strong_out", "out"}:
        next_checks.append("观察后续 5m/15m 资金能否由流出转为中性或流入。")
    elif flow_level in {"strong_in", "in"}:
        next_checks.append("观察资金流入是否能延续，并避免价格冲高回落。")
    if not next_checks:
        next_checks.append("继续观察价格是否突破压力或跌破支撑，等待资金/价格共振。")

    return {
        "trend": trend,
        "funds": flow_label,
        "funds_level": flow_level,
        "funds_ratio": flow_att.get("ratio"),
        "funds_source": flow_basis,
        "funds_source_label": funds_source_label,
        "funds_latest_time": row.get("flow_latest_time") or row.get("flow_trade_date") or "",
        "funds_age_seconds": row.get("flow_age_seconds"),
        "heat": heat,
        "heat_score": heat_score,
        "operation_advice": advice,
        "risk_tip": "、".join(risk_parts[:3]),
        "drawdown_guard": {
            "level": guard_level,
            "action": guard_action,
            "reason": guard_reason,
            "stop_loss_line": stop_loss_line,
            "reduce_line": reduce_line,
        },
        "freshness": freshness,
        "confidence": {"score": confidence, "label": confidence_label},
        "decision_path": [
            f"结论：{advice}",
            f"触发规则：{decision_rule}",
            f"回撤规则：{guard_action} - {guard_reason}",
        ],
        "evidence": evidence,
        "data_quality": {
            "label": "可用" if confidence >= 55 else "谨慎",
            "flags": data_flags,
            "quote_status": row.get("quote_status") or "missing",
            "flow_status": flow_status or "missing",
            "quote_time": row.get("quote_snapshot_at") or row.get("snapshot_at") or row.get("quote_trade_date") or "",
            "flow_time": row.get("flow_latest_time") or row.get("flow_trade_date") or "",
        },
        "next_checks": next_checks,
    }


def _portfolio_refresh_qmt_min_flow(codes: list[str], *, force: bool = False) -> dict[str, object]:
    clean = _safe_portfolio_stock_codes(codes)
    if not clean:
        return {"status": "empty", "rows": 0}
    cache_key = f"portfolio_qmt_flow_min_{','.join(clean)}"
    if not force:
        cached = _cache_get(cache_key, ttl_seconds=_trading_live_ttl_seconds(60, intraday_seconds=30))
        if cached is not None:
            return cached

    try:
        from integrations.qmt import bridge
        from integrations.qmt.backend import to_qmt_symbol
    except Exception as exc:
        result = {"status": "unavailable", "rows": 0, "error": str(exc)[:160]}
        _cache_set(cache_key, result)
        return result

    qmt_codes = [symbol for symbol in (to_qmt_symbol(code) for code in clean) if symbol]
    if not qmt_codes:
        result = {"status": "empty", "rows": 0}
        _cache_set(cache_key, result)
        return result

    trade_date = date.today().isoformat()
    try:
        df = bridge.flow_min(
            qmt_codes,
            trade_date=trade_date,
            batch_size=80,
            timeout=max(5, int(os.environ.get("QMT_FLOW_MIN_TIMEOUT", "30"))),
        )
    except Exception as exc:
        result = {"status": "error", "rows": 0, "error": str(exc)[:200]}
        _cache_set(cache_key, result)
        return result
    if df is None or df.empty:
        result = {"status": "empty", "rows": 0}
        _cache_set(cache_key, result)
        return result

    now = datetime.now().replace(microsecond=0)
    out = df.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
    if "qmt_code" not in out.columns:
        out["qmt_code"] = out["stock_code"].map(lambda code: to_qmt_symbol(str(code)) or "")
    out["trade_time"] = pd.to_datetime(out["trade_time"], errors="coerce")
    out = out[out["trade_time"].notna()]
    out = out[out["stock_code"].isin(clean)]
    if out.empty:
        result = {"status": "empty", "rows": 0}
        _cache_set(cache_key, result)
        return result
    out["snapshot_at"] = now
    out["etl_sync_at"] = now
    out["data_source"] = "gj_qmt"
    out["source_time"] = out["trade_time"]
    out["received_at"] = now
    out["batch_id"] = f"qmt_flow_min_{now.strftime('%Y%m%d%H%M%S')}"
    out["quality_status"] = "PENDING"
    out["permission_status"] = "SUPPORTED"
    keep_cols = [
        "stock_code", "trade_time", "main_net_inflow", "max_net_inflow", "lg_net_inflow",
        "mid_net_inflow", "sm_net_inflow", "snapshot_at", "etl_sync_at", "qmt_code",
        "data_source", "source_time", "received_at", "batch_id", "quality_status", "permission_status",
    ]
    out = out[[col for col in keep_cols if col in out.columns]]
    rows = _df_to_records(out)
    if not rows:
        result = {"status": "empty", "rows": 0}
        _cache_set(cache_key, result)
        return result

    flow_engine = get_minute_engine()
    with flow_engine.begin() as conn:
        for chunk_start in range(0, len(rows), 1000):
            chunk = rows[chunk_start : chunk_start + 1000]
            clauses = []
            params = {}
            for idx, row in enumerate(chunk):
                clauses.append(f"(stock_code = :c{idx} AND trade_time = :t{idx})")
                params[f"c{idx}"] = row["stock_code"]
                params[f"t{idx}"] = row["trade_time"]
            if clauses:
                conn.execute(text("DELETE FROM sm_stock_capital_flow_min WHERE " + " OR ".join(clauses)), params)
    write_frame(
        pd.DataFrame(rows),
        "sm_stock_capital_flow_min",
        flow_engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    result = {"status": "success", "rows": len(rows), "generated_at": now.isoformat(timespec="seconds")}
    _cache_set(cache_key, result)
    return result


def _portfolio_min_flow_summary(
    codes: list[str],
    *,
    trade_date: str | None = None,
    market_mode: str | None = None,
) -> dict[str, dict]:
    """Return one correctly aligned capital-flow snapshot per stock.

    ``sm_stock_capital_flow_min`` stores a cumulative value for the trading
    day.  Window flow therefore has to be calculated as ``latest - baseline``;
    summing rows multiplies the same day-to-date flow several times and can
    even reverse the apparent direction.
    """
    clean = _safe_portfolio_stock_codes(codes)
    if not clean:
        return {}
    mode = str(market_mode or _portfolio_market_mode())
    target_date = str(
        trade_date
        or (date.today().isoformat() if mode == "intraday" else _portfolio_close_trade_date())
    )[:10]
    placeholders = ", ".join([f":code_{idx}" for idx, _ in enumerate(clean)])
    params = {f"code_{idx}": code for idx, code in enumerate(clean)}
    params["flow_date"] = target_date
    rows = _read_sql(
        f"""
        SELECT stock_code, trade_time, main_net_inflow, max_net_inflow,
               lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
        FROM sm_stock_capital_flow_min
        WHERE stock_code IN ({placeholders})
          AND trade_time >= :flow_date
          AND trade_time < DATE_ADD(:flow_date, INTERVAL 1 DAY)
        ORDER BY stock_code, trade_time
        """,
        params,
    )

    grouped: dict[str, list[tuple[datetime, dict]]] = {}
    for item in rows:
        code = str(item.get("stock_code") or "").strip().zfill(6)
        raw_time = item.get("trade_time")
        point_time = raw_time.replace(tzinfo=None) if isinstance(raw_time, datetime) else None
        if point_time is None and raw_time:
            try:
                point_time = datetime.strptime(str(raw_time)[:19], "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                continue
        if code and point_time is not None and item.get("main_net_inflow") is not None:
            grouped.setdefault(code, []).append((point_time, item))

    def _window_delta(
        points: list[tuple[datetime, dict]],
        latest_time: datetime,
        latest_value: float,
        minutes: int,
    ) -> tuple[float | None, float | None]:
        cutoff = latest_time - timedelta(minutes=minutes)
        baseline_idx = -1
        for idx, (point_time, _) in enumerate(points):
            if point_time <= cutoff:
                baseline_idx = idx
            else:
                break
        if baseline_idx < 0:
            return None, None
        baseline = _portfolio_num(points[baseline_idx][1].get("main_net_inflow"))
        delta = latest_value - baseline
        absolute_moves = 0.0
        previous = baseline
        for _, point in points[baseline_idx + 1 :]:
            current = _portfolio_num(point.get("main_net_inflow"))
            absolute_moves += abs(current - previous)
            previous = current
        return delta, absolute_moves

    out: dict[str, dict] = {}
    for code, points in grouped.items():
        points.sort(key=lambda pair: pair[0])
        latest_dt, latest = points[-1]
        latest_time = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
        latest_value = _portfolio_num(latest.get("main_net_inflow"))
        flow_1m, _ = _window_delta(points, latest_dt, latest_value, 1)
        flow_5m, flow_5m_abs = _window_delta(points, latest_dt, latest_value, 5)
        flow_15m, _ = _window_delta(points, latest_dt, latest_value, 15)
        flow_age_seconds = _portfolio_time_age_seconds(latest_dt)
        is_fresh = (
            mode == "intraday"
            and
            flow_age_seconds is not None
            and flow_age_seconds <= PORTFOLIO_FLOW_FRESH_SECONDS
            and latest_time[:10] == datetime.now().date().isoformat()
        )
        is_closed = (
            mode != "intraday"
            and latest_time[:10] == target_date
            and (latest_dt.hour, latest_dt.minute) >= (14, 55)
        )
        status = "fresh" if is_fresh else "closed" if is_closed else "stale"
        has_flow_5m = flow_5m is not None
        attitude = (
            _portfolio_flow_attitude(flow_5m, flow_5m_abs)
            if is_fresh and has_flow_5m
            else {
                "level": "neutral",
                "label": "基线建立中",
                "ratio": None,
            }
            if is_fresh
            else {
                "level": "",
                "label": "",
                "ratio": None,
            }
        )
        out[code] = {
            "main_net_inflow": latest.get("main_net_inflow"),
            "max_net_inflow": latest.get("max_net_inflow"),
            "lg_net_inflow": latest.get("lg_net_inflow"),
            "mid_net_inflow": latest.get("mid_net_inflow"),
            "sm_net_inflow": latest.get("sm_net_inflow"),
            "flow_1m": flow_1m,
            "flow_5m": flow_5m,
            "flow_15m": flow_15m,
            "flow_latest_time": latest_time,
            "flow_trade_date": latest_time[:10],
            "flow_age_seconds": flow_age_seconds,
            "flow_status": status,
            "flow_attitude": attitude["level"],
            "flow_attitude_label": attitude["label"],
            "flow_attitude_ratio": attitude["ratio"],
            "flow_attitude_basis": (
                "minute_5m_fresh"
                if is_fresh and has_flow_5m
                else "minute_current_fresh"
                if is_fresh
                else "minute_day_close"
                if is_closed
                else ""
            ),
            "flow_source": latest.get("data_source") or "minute_flow",
            "expected_flow_date": target_date,
        }
    return out


def _portfolio_prev_close(cur_price: float, price_change, pre_close, change_pct) -> float | None:
    """昨收价：优先「现价-涨跌额」(与行情同源)，再日K pre_close，最后涨跌幅反推。"""
    pr = float(cur_price or 0)
    if pr <= 0:
        return None
    if price_change is not None:
        pc = pr - float(price_change)
        if pc > 0:
            return pc
    if pre_close is not None:
        pc = float(pre_close)
        if pc > 0:
            return pc
    chg = float(change_pct or 0)
    if chg == -100:
        return None
    return pr / (1 + chg / 100)


def _portfolio_today_trades(code: str, trade_date: str | None = None) -> list[dict]:
    _ensure_portfolio_trans_log_table()
    try:
        if trade_date:
            return _read_sql("""
                SELECT trans_type, price, shares, source
                FROM st_portfolio_trans_log
                WHERE stock_code = :c AND trans_date = :d
                ORDER BY created_at, id
            """, {"c": code, "d": str(trade_date)[:10]})
        return _read_sql("""
            SELECT trans_type, price, shares, source
            FROM st_portfolio_trans_log
            WHERE stock_code = :c AND trans_date = CURDATE()
            ORDER BY created_at, id
        """, {"c": code})
    except Exception:
        return []


def _portfolio_is_today_buy_position(row: dict | None, trade_date: str | None = None) -> bool:
    if not row:
        return False
    pos_date = str(row.get("position_date") or "")[:10]
    src = str(row.get("position_source") or "").lower()
    target_date = str(trade_date or date.today().isoformat())[:10]
    return src == "today_buy" and pos_date == target_date


def _portfolio_effective_today_trades(
    stock_code: str,
    shares: int,
    cost_price: float,
    position_row: dict | None = None,
    trade_date: str | None = None,
) -> list[dict]:
    trades = _portfolio_today_trades(stock_code, trade_date) if stock_code else []
    if trades:
        return trades
    if _portfolio_is_today_buy_position(position_row, trade_date) and int(shares or 0) > 0 and float(cost_price or 0) > 0:
        return [{
            "trans_type": "buy",
            "price": float(cost_price),
            "shares": int(shares),
            "source": "position_add",
        }]
    return []


def _portfolio_today_trade_state(shares: int, trades: list[dict] | None) -> dict:
    sh = int(shares or 0)
    buy_qty = sell_qty = 0
    buy_amount = sell_amount = 0.0
    for row in trades or []:
        qty = int(row.get("shares") or 0)
        px = float(row.get("price") or 0)
        if qty <= 0:
            continue
        if str(row.get("trans_type", "")).lower() == "sell":
            sell_qty += qty
            sell_amount += max(px, 0) * qty
        else:
            buy_qty += qty
            buy_amount += max(px, 0) * qty

    start_shares = max(0, sh - buy_qty + sell_qty)
    has_today_trade = buy_qty > 0 or sell_qty > 0
    is_today_open = start_shares == 0 and buy_qty > 0 and sh > 0
    is_today_cleared = sh == 0 and sell_qty > 0 and (start_shares > 0 or buy_qty > 0)
    is_today_reopened = start_shares > 0 and buy_qty > 0 and sell_qty >= start_shares and sh > 0

    status = "holding" if sh > 0 else "watch"
    label = ""
    if is_today_cleared:
        status = "today_cleared"
        label = "今日清仓" if start_shares > 0 else "日内清仓"
    elif is_today_reopened:
        status = "today_reopened"
        label = "今日重开"
    elif is_today_open:
        status = "today_open"
        label = "今日开仓"
    elif has_today_trade:
        status = "today_traded"
        label = "今日交易"

    return {
        "today_start_shares": start_shares,
        "today_buy_shares": buy_qty,
        "today_sell_shares": sell_qty,
        "today_buy_amount": round(buy_amount, 2),
        "today_sell_amount": round(sell_amount, 2),
        "has_today_trade": has_today_trade,
        "is_today_open": is_today_open,
        "is_today_cleared": is_today_cleared,
        "is_today_reopened": is_today_reopened,
        "today_position_status": status,
        "today_position_label": label,
    }


def _portfolio_day_profit(
    shares: int,
    cur_price: float,
    live_price,
    price_change,
    pre_close,
    change_pct,
    stock_code: str = "",
    trades: list[dict] | None = None,
    trade_date: str | None = None,
) -> float | None:
    """
    当日盈亏（对齐东方财富常见口径）：
    无当日成交时 ≈ 持股数量 × 涨跌额 = (现价 - 昨收) × 股数；
    有当日成交时按昨日持仓、今日买入、今日卖出逐笔配对，清仓票保留已实现盈亏。
    现价与涨跌额必须同源，禁止「实时价 + 过期日K昨收」混用。
    """
    sh = int(shares or 0)
    pr = float(cur_price or 0)
    if pr <= 0:
        return None
    chg_amt = None
    if price_change is not None:
        chg_amt = float(price_change)
    elif live_price is None and pre_close is not None:
        pc = float(pre_close)
        if pc > 0:
            chg_amt = pr - pc
    if chg_amt is None:
        pc = _portfolio_prev_close(pr, None, pre_close, change_pct)
        if not pc or pc <= 0:
            return None
        chg_amt = pr - pc
    prev_close = pr - chg_amt
    if prev_close <= 0:
        return round(sh * chg_amt, 2)

    trades = list(trades) if trades is not None else (_portfolio_today_trades(stock_code, trade_date) if stock_code else [])
    if sh <= 0 and not trades:
        return None

    buy_qty = 0
    sell_qty = 0
    for row in trades or []:
        qty = int(row.get("shares") or 0)
        if qty <= 0:
            continue
        if str(row.get("trans_type", "")).lower() == "sell":
            sell_qty += qty
        else:
            buy_qty += qty

    start_shares = max(0, sh - buy_qty + sell_qty)
    lots = []
    if start_shares > 0:
        lots.append({"qty": start_shares, "basis": prev_close})

    realized = 0.0
    for row in trades or []:
        px = float(row.get("price") or 0)
        qty = int(row.get("shares") or 0)
        if px <= 0 or qty <= 0:
            continue
        if str(row.get("trans_type", "")).lower() != "sell":
            lots.append({"qty": qty, "basis": px})
            realized -= _portfolio_trade_fee("buy", px, qty)
            continue

        remaining = qty
        realized -= _portfolio_trade_fee("sell", px, qty)
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, int(lot["qty"]))
            realized += (px - float(lot["basis"])) * matched
            lot["qty"] -= matched
            remaining -= matched
            if lot["qty"] <= 0:
                lots.pop(0)
        if remaining > 0:
            realized += (px - prev_close) * remaining

    unrealized = sum((pr - float(lot["basis"])) * int(lot["qty"]) for lot in lots if int(lot["qty"]) > 0)
    day_pnl = realized + unrealized
    return round(day_pnl, 2)


def _portfolio_snapshot_ttl_seconds(live_mode: bool) -> int:
    if live_mode:
        # The live page polls frequently, but rebuilding the full portfolio
        # snapshot on every request overloads both MySQL and the QMT bridge.
        # Keep the API responsive while still allowing sub-second UI polling
        # to observe the latest persisted snapshot.
        return _trading_live_ttl_seconds(60, intraday_seconds=3)
    return 30 if _is_monitor_trading_time() else 120


def _portfolio_apply_snapshot_quote(
    row: dict,
    *,
    portfolio_mode: str,
    close_trade_date: str,
    kline: dict | None,
    live_quote: dict | None = None,
    closed_quote: dict | None = None,
) -> None:
    """Apply the freshest valid quote for intraday or post-close display."""
    kline = kline or {}
    kline_trade_date = str(kline.get("trade_date") or "")[:10]
    kline_is_close = bool(kline and kline_trade_date == close_trade_date)

    if portfolio_mode == "intraday":
        _portfolio_apply_quote(row, live_quote)
    elif closed_quote and str(closed_quote.get("snapshot_at") or "")[:10] == close_trade_date:
        _portfolio_apply_quote(row, closed_quote)
    elif kline_is_close:
        _portfolio_apply_kline_quote(row, kline, status="closed")
    else:
        _portfolio_apply_kline_quote(row, kline, status="previous_close")

    _portfolio_rebase_quote_change(row, kline)


def _build_portfolio_snapshot(*, force_live: bool = False) -> dict:
    # Schema maintenance belongs to startup/migration paths.  Doing
    # information_schema checks (and potentially ALTER TABLE) in a hot read
    # endpoint makes a transient database lock visible as a page timeout.
    rows = _read_sql("""
        SELECT p.* FROM st_user_portfolio p
        ORDER BY (p.shares > 0) DESC, p.sort_order, p.id
    """)
    if not rows:
        return {"data": [], "total": 0, "summary": {}}

    codes, placeholders, code_params = _portfolio_stock_code_query(
        (row.get("stock_code") for row in rows),
        prefix="snapshot_code",
    )
    if not codes:
        return {"data": rows, "total": len(rows), "summary": {}}

    kline_map = {}
    try:
        kline_rows = _read_sql(f"""
            SELECT stock_code, trade_date, close, change_pct, short_name, pre_close
            FROM sm_stock_kline
            WHERE k_type = 1 AND stock_code IN ({placeholders})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
            ORDER BY stock_code, trade_date DESC
        """, code_params)
        for row in kline_rows:
            stock_code = str(row["stock_code"])
            if stock_code not in kline_map:
                kline_map[stock_code] = row
    except Exception as exc:
        _record_fallback('_build_portfolio_snapshot:5137', exc)

    portfolio_mode = _portfolio_market_mode()
    close_trade_date = _portfolio_close_trade_date()
    flow_target_date = date.today().isoformat() if portfolio_mode == "intraday" else close_trade_date
    closed_quotes = {}
    try:
        # A page read must never synchronously enter the QMT SDK.  The QMT
        # runtime updates sm_stock_current in the background; if it is down,
        # the helper returns a bounded stale quote and the UI can show that
        # status instead of waiting for the provider timeout.
        live_quotes = _portfolio_fetch_live_quotes(codes, force=force_live) if portfolio_mode == "intraday" else {}
        if portfolio_mode != "intraday":
            closed_quotes = _portfolio_closed_quotes_from_current_table(codes, close_trade_date)
    except Exception:
        live_quotes = {}
        closed_quotes = {}
    # Minute-flow refresh is a provider call plus a write/delete cycle.  It is
    # intentionally excluded from the read path; the scheduler/capital-flow
    # endpoint owns that refresh and this page only reads persisted rows.
    flow_min_refresh = {"status": "cached_only", "rows": 0}
    flow_min_map = _portfolio_min_flow_summary(
        codes,
        trade_date=flow_target_date,
        market_mode=portfolio_mode,
    )

    flow_map: dict[str, dict] = {}
    try:
        flow_rows = _read_sql(f"""
            SELECT f.stock_code, f.trade_date, f.main_net_inflow, f.max_net_inflow,
                   f.lg_net_inflow, f.mid_net_inflow, f.sm_net_inflow, f.data_source
            FROM sm_stock_capital_flow_daily f
            WHERE f.stock_code IN ({placeholders})
              AND f.trade_date = :flow_date
        """, {**code_params, "flow_date": flow_target_date})
        for item in flow_rows:
            flow_map[str(item.get("stock_code") or "").strip().zfill(6)] = item
    except Exception:
        flow_map = {}

    profile_map: dict[str, dict] = {}
    try:
        profile_rows = _read_sql(f"""
            SELECT stock_code, industry AS industry_name
            FROM sm_stock_snapshot
            WHERE stock_code IN ({placeholders})
        """, code_params)
        for item in profile_rows:
            profile_map[str(item.get("stock_code") or "").strip().zfill(6)] = {
                "industry_name": item.get("industry_name") or "",
            }
    except Exception:
        profile_map = {}
    try:
        concept_rows = _read_sql(f"""
            SELECT stock_code, concept_tag, pop_tag
            FROM st_hot_rank_ths
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_ths)
              AND stock_code IN ({placeholders})
        """, code_params)
        for item in concept_rows:
            stock_code = str(item.get("stock_code") or "").strip().zfill(6)
            profile_map.setdefault(stock_code, {})
            profile_map[stock_code]["concept_tag"] = item.get("concept_tag") or ""
            profile_map[stock_code]["pop_tag"] = item.get("pop_tag") or ""
    except Exception as exc:
        _record_fallback('_build_portfolio_snapshot:5196', exc)

    tech_cache_key = "portfolio_tech_risk_signal"
    tech_risk_news_signal = _cache_get(
        tech_cache_key,
        ttl_seconds=_trading_live_ttl_seconds(900, intraday_seconds=900),
    )
    if tech_risk_news_signal is None:
        tech_risk_news_signal = {
            "status": "skipped",
            "triggered": False,
            "headline": "自选股刷新跳过慢风险扫描",
            "summary": "行情刷新优先返回，技术风险信号由独立页面/缓存更新。",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    today_trades_map = {}
    try:
        all_trades = _read_sql(f"""
            SELECT stock_code, trans_type, price, shares, source
            FROM st_portfolio_trans_log
            WHERE stock_code IN ({placeholders}) AND trans_date = CURDATE()
            ORDER BY stock_code, created_at, id
        """, code_params)
        for row in all_trades:
            stock_code = str(row["stock_code"]).strip().zfill(6)
            today_trades_map.setdefault(stock_code, []).append(row)
    except Exception as exc:
        _record_fallback('_build_portfolio_snapshot:5216', exc)

    total_hold_profit = 0.0
    today_hold_profit = 0.0
    holding_count = 0
    today_open_count = 0
    today_cleared_count = 0
    watch_advice_counts: dict[str, int] = {}
    drawdown_guard_counts: dict[str, int] = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "DATA": 0}
    quote_status_counts: dict[str, int] = {}
    quote_source_counts: dict[str, int] = {}
    quote_moments: list[str] = []
    for row in rows:
        stock_code = str(row.get("stock_code") or "").strip().zfill(6)
        profile = profile_map.get(stock_code) or {}
        row["industry_name"] = profile.get("industry_name") or row.get("industry_name") or ""
        row["concept_tag"] = profile.get("concept_tag") or row.get("concept_tag") or ""
        row["pop_tag"] = profile.get("pop_tag") or row.get("pop_tag") or ""
        row["macro_risk_triggered"] = bool(
            tech_risk_news_signal.get("triggered")
            and int(row.get("shares") or 0) > 0
            and holding_matches_signal(row, tech_risk_news_signal)
        )
        row["macro_risk_reason"] = (
            tech_risk_news_signal.get("action") if row["macro_risk_triggered"] else ""
        )
        kline = kline_map.get(stock_code, {})
        row["kline_pre_close"] = kline.get("pre_close")
        row["kline_close"] = kline.get("close")
        row["kline_trade_date"] = str(kline.get("trade_date", ""))[:10] if kline.get("trade_date") else None
        target_trade_date = _portfolio_quote_trade_date(row)
        _portfolio_apply_snapshot_quote(
            row,
            portfolio_mode=portfolio_mode,
            close_trade_date=close_trade_date,
            kline=kline,
            live_quote=live_quotes.get(stock_code),
            closed_quote=closed_quotes.get(stock_code),
        )
        flow = flow_map.get(stock_code) or {}
        row["main_net_inflow"] = flow.get("main_net_inflow")
        row["max_net_inflow"] = flow.get("max_net_inflow")
        row["lg_net_inflow"] = flow.get("lg_net_inflow")
        row["mid_net_inflow"] = flow.get("mid_net_inflow")
        row["sm_net_inflow"] = flow.get("sm_net_inflow")
        row["flow_trade_date"] = str(flow.get("trade_date", ""))[:10] if flow.get("trade_date") else ""
        row["flow_source"] = flow.get("data_source") or ""
        row["expected_flow_date"] = flow_target_date
        row["flow_status"] = "closed" if flow and portfolio_mode != "intraday" else "stale" if flow else "missing"
        row["flow_attitude"] = ""
        row["flow_attitude_label"] = ""
        row["flow_attitude_ratio"] = None
        row["flow_attitude_basis"] = ""
        min_flow = flow_min_map.get(stock_code) or {}
        row["flow_1m"] = min_flow.get("flow_1m")
        row["flow_5m"] = min_flow.get("flow_5m")
        row["flow_15m"] = min_flow.get("flow_15m")
        row["flow_latest_time"] = min_flow.get("flow_latest_time") or ""
        row["flow_age_seconds"] = min_flow.get("flow_age_seconds")
        min_status = str(min_flow.get("flow_status") or "")
        if min_status in {"fresh", "closed"}:
            for field in ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"):
                if min_flow.get(field) is not None:
                    row[field] = min_flow.get(field)
            row["flow_trade_date"] = min_flow.get("flow_trade_date") or row["flow_trade_date"]
            row["flow_source"] = min_flow.get("flow_source") or row["flow_source"]
            row["flow_status"] = min_status
            row["flow_attitude"] = min_flow.get("flow_attitude") or ""
            row["flow_attitude_label"] = min_flow.get("flow_attitude_label") or ""
            row["flow_attitude_ratio"] = min_flow.get("flow_attitude_ratio")
            row["flow_attitude_basis"] = min_flow.get("flow_attitude_basis") or ""
        elif min_flow and not flow:
            # Keep the timestamp for diagnostics, but never turn an incomplete
            # or old snapshot into a current trading signal.
            row["flow_status"] = "stale"
            row["flow_trade_date"] = min_flow.get("flow_trade_date") or ""
            row["flow_source"] = min_flow.get("flow_source") or ""
        if row["flow_status"] == "closed" and row.get("main_net_inflow") is not None:
            daily_attitude = _portfolio_daily_flow_attitude(row.get("main_net_inflow"), row.get("quote_amount") or row.get("amount"))
            row["flow_attitude"] = daily_attitude["level"]
            row["flow_attitude_label"] = daily_attitude["label"]
            row["flow_attitude_ratio"] = daily_attitude["ratio"]
            row["flow_attitude_basis"] = row["flow_attitude_basis"] or "daily_close"
        if row["flow_status"] not in {"fresh", "closed"}:
            for field in (
                "main_net_inflow", "max_net_inflow", "lg_net_inflow",
                "mid_net_inflow", "sm_net_inflow", "flow_1m", "flow_5m", "flow_15m",
            ):
                row[field] = None
        row["flow_is_stale"] = row["flow_status"] in {"stale", "missing"}
        if row.get("cur_price") is None:
            fallback_status = "stale" if portfolio_mode == "intraday" else "previous_close"
            _portfolio_apply_kline_quote(row, kline, status=fallback_status)
        if row.get("change_pct") is None:
            row["change_pct"] = kline.get("change_pct")
        if row.get("current_name") is None:
            row["current_name"] = kline.get("short_name")
        row["quote_status"] = row.get("quote_status") or "missing"
        quote_status = str(row.get("quote_status") or "missing")
        quote_status_counts[quote_status] = quote_status_counts.get(quote_status, 0) + 1
        quote_source = str(row.get("quote_source") or "missing")
        quote_source_counts[quote_source] = quote_source_counts.get(quote_source, 0) + 1
        quote_moment = str(row.get("quote_snapshot_at") or row.get("quote_trade_date") or "")
        if quote_moment:
            quote_moments.append(quote_moment)

        trade_date = str(row.get("quote_trade_date") or target_trade_date or _portfolio_quote_trade_date(row))[:10]
        row["quote_trade_date"] = trade_date
        cost_price = float(row.get("cost_price") or 0)
        current_price = float(row.get("cur_price") or 0) if row.get("cur_price") is not None else 0.0
        shares = int(row.get("shares") or 0)
        trades = today_trades_map.get(stock_code, [])
        if not trades and _portfolio_is_today_buy_position(row, trade_date) and shares > 0 and cost_price > 0:
            trades = [{"trans_type": "buy", "price": cost_price, "shares": shares, "source": "position_add"}]

        trade_state = _portfolio_today_trade_state(shares, trades)
        row.update(trade_state)
        row["is_holding"] = shares > 0
        hold_profit = _portfolio_cost_profit(shares, current_price, cost_price)
        row["profit"] = hold_profit
        row["profit_pct"] = round((current_price / cost_price - 1) * 100, 2) if shares > 0 and current_price > 0 and cost_price > 0 else None
        row["today_profit"] = _portfolio_day_profit(
            shares,
            current_price,
            row.get("live_price"),
            row.get("price_change"),
            row.get("quote_prev_close") or row.get("kline_pre_close"),
            row.get("change_pct"),
            row["stock_code"],
            trades,
            trade_date,
        )
        row["display_name"] = row.get("current_name") or row.get("short_name") or row["stock_code"]
        row["snapshot_at"] = row.get("quote_snapshot_at") or ""
        row["watch_analysis"] = _portfolio_build_watch_analysis(row)
        advice = str((row["watch_analysis"] or {}).get("operation_advice") or "")
        if advice:
            watch_advice_counts[advice] = watch_advice_counts.get(advice, 0) + 1
        guard_level = str(((row["watch_analysis"] or {}).get("drawdown_guard") or {}).get("level") or "LOW").upper()
        drawdown_guard_counts[guard_level] = drawdown_guard_counts.get(guard_level, 0) + 1

        if row.get("is_today_open") or row.get("is_today_reopened"):
            today_open_count += 1
        if row.get("is_today_cleared"):
            today_cleared_count += 1
        if shares > 0:
            holding_count += 1
            if hold_profit is not None:
                total_hold_profit += hold_profit
        if row["today_profit"] is not None:
            today_hold_profit += row["today_profit"]

    tech_risk_signal = dict(tech_risk_news_signal or {})
    exposed_holdings = [
        {
            "stock_code": str(row.get("stock_code") or ""),
            "short_name": row.get("display_name") or row.get("short_name") or "",
            "reason": row.get("macro_risk_reason") or tech_risk_signal.get("headline") or "",
        }
        for row in rows
        if row.get("macro_risk_triggered")
    ]
    tech_risk_signal["exposed_holdings"] = exposed_holdings
    tech_risk_signal["exposed_holding_count"] = len(exposed_holdings)

    return {
        "data": rows,
        "total": len(rows),
        "summary": {
            "holding_count": holding_count,
            "total_hold_profit": round(total_hold_profit, 2),
            "today_hold_profit": round(today_hold_profit, 2),
            "today_open_count": today_open_count,
            "today_cleared_count": today_cleared_count,
            "quote_source": (
                next(iter(quote_source_counts))
                if len(quote_source_counts) == 1
                else "mixed"
            ),
            "quote_source_counts": quote_source_counts,
            "quote_generated_at": max(quote_moments) if quote_moments else "",
            "quote_status_counts": quote_status_counts,
            "flow_min_refresh": flow_min_refresh,
            "watch_advice_counts": watch_advice_counts,
            "drawdown_guard_counts": drawdown_guard_counts,
            "drawdown_guard_alerts": int(drawdown_guard_counts.get("HIGH", 0) + drawdown_guard_counts.get("MEDIUM", 0)),
            "tech_risk_signal": tech_risk_signal,
            "tech_risk_alerts": int(tech_risk_signal.get("exposed_holding_count") or 0) if tech_risk_signal.get("triggered") else 0,
        },
    }


def _get_portfolio_snapshot(
    live_mode: bool,
    *,
    force_live: bool = False,
    force_request_id: str = "",
) -> dict:
    ttl_seconds = _portfolio_snapshot_ttl_seconds(live_mode)
    cache_key = "portfolio_snapshot"
    force_request_id = str(force_request_id or "").strip()[:128] if force_live else ""
    completed_force = _portfolio_completed_force_result(force_request_id)
    if completed_force is not None:
        if live_mode:
            return {
                **completed_force,
                "live": True,
                "force": True,
                "force_reused": True,
            }
        return completed_force
    entry = _cache_peek(cache_key)
    stale = entry[1] if entry is not None else None
    if not force_live and _portfolio_snapshot_entry_is_fresh(entry, ttl_seconds):
        if live_mode:
            return {**entry[1], "live": True}
        return entry[1]

    # The live page can poll frequently and multiple browser sessions may do so
    # together.  Exactly one request may run the multi-query builder.  Other
    # stale normal reads return immediately.  A cold or forced read may wait a
    # tightly bounded interval so the usual ~100 ms build can finish, but a
    # wedged builder cannot exhaust the FastAPI threadpool.
    if stale is not None and not force_live:
        acquired = _portfolio_snapshot_build_lock.acquire(blocking=False)
    else:
        acquired = _portfolio_snapshot_build_lock.acquire(
            timeout=PORTFOLIO_SNAPSHOT_LOCK_WAIT_SECONDS,
        )
    if not acquired:
        if stale is not None and not force_live and "error" not in stale:
            return {
                **stale,
                **({"live": True} if live_mode else {}),
                "snapshot_stale": True,
                "snapshot_refreshing": True,
            }
        return {
            "data": stale.get("data", []) if isinstance(stale, dict) else [],
            "total": stale.get("total", 0) if isinstance(stale, dict) else 0,
            "summary": stale.get("summary", {}) if isinstance(stale, dict) else {},
            "live": bool(live_mode),
            "error": "portfolio snapshot refresh is already in progress",
            "retryable": True,
            "snapshot_stale": stale is not None,
            "snapshot_refreshing": True,
            "force": force_live,
        }
    try:
        # A same-id force request may have finished while this retry waited for
        # the build lock.  Recheck after acquiring it so the waiter cannot turn
        # a just-completed idempotent refresh into a second full rebuild.
        if force_live and force_request_id:
            completed_force = _portfolio_completed_force_result(force_request_id)
            if completed_force is not None:
                if live_mode:
                    return {
                        **completed_force,
                        "live": True,
                        "force": True,
                        "force_reused": True,
                    }
                return completed_force
        # Recheck after winning the lock because another request may have
        # published between our first cache read and this nonblocking acquire.
        if not force_live:
            refreshed_entry = _cache_peek(cache_key)
            if _portfolio_snapshot_entry_is_fresh(refreshed_entry, ttl_seconds):
                if live_mode:
                    return {**refreshed_entry[1], "live": True}
                return refreshed_entry[1]
        with _cache_lock:
            build_generation = _portfolio_snapshot_generation
        try:
            result = _build_portfolio_snapshot(force_live=force_live)
            if "error" in result:
                result = {**result, "retryable": True}
            if not _portfolio_snapshot_cache_publish(
                result,
                generation=build_generation,
                force_request_id=(
                    force_request_id
                    if force_live and "error" not in result
                    else ""
                ),
            ):
                return {
                    "data": [],
                    "total": 0,
                    "summary": {},
                    "live": bool(live_mode),
                    "error": "portfolio snapshot was invalidated during refresh",
                    "retryable": True,
                    "snapshot_superseded": True,
                    "force": force_live,
                }
            if live_mode:
                return {**result, "live": True, "force": force_live}
            return result
        except Exception as exc:
            with _cache_lock:
                generation_is_current = build_generation == _portfolio_snapshot_generation
            if stale is not None and generation_is_current and "error" not in stale:
                fallback = {
                    **stale,
                    "snapshot_stale": True,
                    "snapshot_error": str(exc)[:200],
                    "retryable": True,
                }
                _portfolio_snapshot_cache_publish(
                    fallback,
                    generation=build_generation,
                )
                response = {
                    **fallback,
                    **({"live": True} if live_mode else {}),
                    "force": force_live,
                }
                if force_live:
                    response["error"] = (
                        "portfolio snapshot refresh failed: " + str(exc)[:160]
                    )
                return response
            failure = {
                "data": [],
                "total": 0,
                "summary": {},
                "error": str(exc)[:200],
                "retryable": True,
            }
            if generation_is_current:
                _portfolio_snapshot_cache_publish(
                    failure,
                    generation=build_generation,
                )
            else:
                failure["snapshot_superseded"] = True
            return failure
    finally:
        _portfolio_snapshot_build_lock.release()


def _invalidate_portfolio_snapshot_cache() -> None:
    global _portfolio_snapshot_generation
    with _cache_lock:
        _portfolio_snapshot_generation += 1
        _cache_store.pop("portfolio_snapshot", None)
        _portfolio_completed_force_requests.clear()


def _invalidate_market_runtime_caches() -> None:
    _cache_drop_prefix("portfolio_live_quotes_")
    _invalidate_portfolio_snapshot_cache()
    _cache_drop_prefix("monitor_data_")
    _cache_drop_prefix("sector_movement_")


@router.get("/portfolio/list")
def portfolio_list():
    """持仓列表（带实时行情）"""
    return _get_portfolio_snapshot(live_mode=False)


@router.get("/portfolio/codes")
def portfolio_codes():
    """Return the production watchlist used by the external BigQMT collector."""
    rows = _read_sql(
        "SELECT stock_code FROM st_user_portfolio "
        "WHERE stock_code IS NOT NULL AND stock_code <> '' ORDER BY sort_order, id"
    )
    codes = _safe_portfolio_stock_codes(row.get("stock_code") for row in rows)
    return {"data": [{"stock_code": code} for code in codes], "total": len(codes)}


@router.post("/portfolio/add")
def portfolio_add(body: PortfolioAdd):
    """添加自选股"""
    try:
        code = _require_portfolio_stock_code(body.stock_code)
        _ensure_portfolio_position_columns()
        _ensure_portfolio_trans_log_table()
        # get name from si_all_code
        names = _read_sql("SELECT short_name FROM si_all_code WHERE stock_code = :c LIMIT 1", {"c": code})
        name = names[0].get("short_name", code) if names else code
        max_order = _read_sql("SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_order FROM st_user_portfolio")
        next_order = int(max_order[0]["next_order"]) if max_order else 0
        _exec_sql("""
            INSERT INTO st_user_portfolio
                (stock_code, short_name, cost_price, shares, position_source, position_date, add_date, sort_order, notes, etl_sync_at)
            VALUES
                (:c, :n, :p, :s, :src, CURDATE(), CURDATE(), :so, :nt, NOW())
            ON DUPLICATE KEY UPDATE
                cost_price=:p, shares=:s, position_source=:src, position_date=CURDATE(), notes=:nt, etl_sync_at=NOW()
        """, {
            "c": code,
            "n": name,
            "p": body.cost_price,
            "s": body.shares,
            "src": "today_buy" if body.is_today_buy else "manual",
            "so": next_order,
            "nt": body.notes,
        })
        _exec_sql("""
            DELETE FROM st_portfolio_trans_log
            WHERE stock_code = :c AND trans_date = CURDATE() AND source = 'position_add'
        """, {"c": code})
        if body.is_today_buy and body.shares > 0 and body.cost_price > 0:
            _portfolio_log_trans(code, "buy", float(body.cost_price), int(body.shares), source="position_add")
        elif not body.is_today_buy and body.shares > 0 and body.cost_price > 0:
            trade_rows = _read_sql(
                "SELECT trans_type, price, shares FROM st_portfolio_trans_log "
                "WHERE stock_code = :c AND source != 'initial' ORDER BY created_at, id",
                {"c": code},
            )
            net_trade_shares = 0
            net_trade_cost = 0.0
            for tr in trade_rows:
                ts = int(tr.get("shares") or 0)
                tp = float(tr.get("price") or 0)
                if str(tr.get("trans_type") or "").lower() == "buy":
                    net_trade_shares += ts
                    net_trade_cost += tp * ts
                else:
                    net_trade_shares -= ts
                    net_trade_cost -= tp * ts
            init_shares = int(body.shares) - net_trade_shares
            if init_shares > 0 and net_trade_shares != 0:
                init_cost = (float(body.cost_price) * int(body.shares) - net_trade_cost) / init_shares
            else:
                init_shares = int(body.shares)
                init_cost = float(body.cost_price)
            _exec_sql("""
                DELETE FROM st_portfolio_trans_log
                WHERE stock_code = :c AND source = 'initial'
            """, {"c": code})
            _portfolio_log_trans(code, "buy", round(init_cost, 4), init_shares, source="initial")
        # 同步更新快照表自选股排序 + 持仓标记
        is_hold = 1 if body.shares > 0 else 0
        _exec_sql("UPDATE sm_stock_snapshot SET sort_order = :so, is_holding = :ih WHERE stock_code = :c",
                  {"so": next_order, "ih": is_hold, "c": code})
        _invalidate_portfolio_snapshot_cache()
        return {"status": "ok", "stock_code": code, "short_name": name}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.delete("/portfolio/remove/{stock_code}")
def portfolio_remove(stock_code: str):
    """删除自选股"""
    try:
        code = _require_portfolio_stock_code(stock_code)
        _exec_sql("DELETE FROM st_user_portfolio WHERE stock_code = :c", {"c": code})
        # 同步更新快照表：移除自选股排序和持仓标记
        _exec_sql("UPDATE sm_stock_snapshot SET sort_order = NULL, is_holding = 0 WHERE stock_code = :c", {"c": code})
        _invalidate_portfolio_snapshot_cache()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


class PortfolioReorder(BaseModel):
    codes: list[str] = []


@router.post("/portfolio/reorder")
def portfolio_reorder(body: PortfolioReorder):
    """拖拽排序"""
    try:
        clean_codes = [_require_portfolio_stock_code(code) for code in body.codes]
        for i, c in enumerate(clean_codes):
            _exec_sql("UPDATE st_user_portfolio SET sort_order = :o WHERE stock_code = :c",
                      {"o": i, "c": c})
            _exec_sql("UPDATE sm_stock_snapshot SET sort_order = :o WHERE stock_code = :c",
                      {"o": i, "c": c})
        _invalidate_portfolio_snapshot_cache()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/portfolio/transact/{stock_code}")
def portfolio_transact(stock_code: str, body: PortfolioTransact):
    """加仓/减仓（东财算法：先写流水，再从流水全量重算成本价）"""
    try:
        code = _require_portfolio_stock_code(stock_code)
        _ensure_portfolio_position_columns()
        _ensure_portfolio_trans_log_table()
        pf = _read_sql("SELECT * FROM st_user_portfolio WHERE stock_code = :c", {"c": code})
        if not pf:
            return {"status": "error", "error": "持仓不存在，请先添加自选股"}
        old = pf[0]
        old_shares = int(old.get("shares") or 0)
        price = float(body.price or 0)
        shares = int(body.shares or 0)

        next_position = _portfolio_calc_next_position(body.trans_type, 0, old_shares, price, shares)
        if next_position.get("status") != "ok":
            return next_position

        trans_type = next_position["trans_type"]
        trade_shares = int(next_position["trade_shares"])

        _portfolio_log_trans(code, trans_type, price, trade_shares)

        # 同步写入操作流水表(st_trade_flow)
        try:
            _watchlist_write_flow(code, old.get("short_name", ""), trans_type, price, trade_shares)
        except Exception as exc:
            _record_fallback('portfolio_transact:5522', exc)

        recalc = portfolio_recalc_cost_from_history(code, _read_sql)
        if recalc.get("status") != "ok":
            return recalc

        new_shares = int(recalc["new_shares"])
        new_cost = float(recalc["new_cost"])

        if trans_type == "buy":
            position_source = "today_buy" if old_shares <= 0 else (old.get("position_source") or "manual")
            position_date_sql = "CURDATE()" if old_shares <= 0 else "position_date"
        else:
            position_source = old.get("position_source") or "manual"
            position_date_sql = "position_date"

        _exec_sql("""
            UPDATE st_user_portfolio
            SET cost_price=:p,
                shares=:s,
                position_source=:src,
                position_date=""" + position_date_sql + """,
                etl_sync_at=NOW()
            WHERE stock_code=:c
        """, {"c": code, "p": new_cost, "s": new_shares, "src": position_source})
        _invalidate_portfolio_snapshot_cache()
        return {"status": "ok", "cost_price": new_cost, "shares": new_shares}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _portfolio_is_trading_day(d: date) -> bool:
    try:
        rows = _read_sql(
            "SELECT trade_status FROM si_trade_calendar WHERE trade_date = :d LIMIT 1",
            {"d": d.isoformat()},
        )
        if rows:
            return int(rows[0].get("trade_status") or 0) == 1
    except Exception as exc:
        _record_fallback('_portfolio_is_trading_day:5562', exc)
    return d.weekday() < 5


def _portfolio_last_trade_date(on_or_before: date | None = None) -> str:
    on_or_before = on_or_before or date.today()
    d_str = on_or_before.isoformat()
    try:
        rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM si_trade_calendar "
            "WHERE trade_status = 1 AND trade_date <= :d",
            {"d": d_str},
        )
        if rows and rows[0].get("d"):
            return str(rows[0]["d"])[:10]
    except Exception as exc:
        _record_fallback('_portfolio_last_trade_date:5578', exc)
    try:
        rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_kline "
            "WHERE k_type = 1 AND trade_date <= :d",
            {"d": d_str},
        )
        if rows and rows[0].get("d"):
            return str(rows[0]["d"])[:10]
    except Exception as exc:
        _record_fallback('_portfolio_last_trade_date:5588', exc)
    return d_str


def _portfolio_market_mode(now=None) -> str:
    """intraday=盘中用现价；post_close=盘后用收盘价"""
    from datetime import datetime as _dt, time as _time

    now = now or _dt.now()
    d = now.date()
    t = now.time()
    if not _portfolio_is_trading_day(d):
        return "post_close"
    if _time(9, 30) <= t < _time(15, 0):
        return "intraday"
    return "post_close"


def _portfolio_close_trade_date(now=None) -> str:
    """盘后分析应对齐的交易日（收盘 bar 的 trade_date）。"""
    from datetime import datetime as _dt, time as _time, timedelta as _td

    now = now or _dt.now()
    d = now.date()
    t = now.time()
    if _portfolio_is_trading_day(d) and t >= _time(15, 0):
        return d.isoformat()
    return _portfolio_last_trade_date(d - _td(days=1))


def _portfolio_kline_bar(code: str, trade_date: str, klines: list | None = None) -> dict | None:
    td = str(trade_date)[:10]
    if klines:
        for k in klines:
            if str(k.get("trade_date", ""))[:10] == td:
                return k
    rows = _read_sql("""
        SELECT trade_date, open, close, high, low, volume, change_pct, turnover_ratio
        FROM sm_stock_kline WHERE stock_code = :c AND k_type = 1 AND trade_date = :d LIMIT 1
    """, {"c": code, "d": td})
    if rows:
        return rows[0]
    try:
        from adata.stock.market.stock_market.stock_market import StockMarket

        df = StockMarket().get_market(
            stock_code=code, start_date=td, end_date=td, k_type=1,
        )
        if df is not None and not df.empty:
            row = df.iloc[-1].to_dict()
            if "trade_date" in row:
                row["trade_date"] = str(row["trade_date"])[:10]
            return row
    except Exception as exc:
        _record_fallback('_portfolio_kline_bar:5642', exc)
    return None


def _portfolio_resolve_quote(code: str, klines: list | None) -> dict:
    """按交易时段选择：盘中现价 / 盘后（或休市）最近有效交易日收盘价。"""
    from datetime import datetime as _dt

    now = _dt.now()
    mode = _portfolio_market_mode(now)
    klines = klines or []

    if mode == "intraday":
        live = None
        try:
            live = _portfolio_fetch_live_quotes([code]).get(code)
        except Exception:
            live = None
        if not live:
            cur_rows = _read_sql("""
                SELECT price, change_pct, snapshot_at
                FROM sm_stock_current WHERE stock_code = :c LIMIT 1
            """, {"c": code})
            if cur_rows and cur_rows[0].get("price") is not None:
                snap = str(cur_rows[0].get("snapshot_at") or "")
                if snap[:10] == now.date().isoformat():
                    live = {**cur_rows[0], "source": "cached"}
        if live and live.get("price") is not None:
            return {
                "mode": "intraday",
                "mode_label": "盘中实时",
                "price": float(live["price"]),
                "change_pct": float(live.get("change_pct") or 0),
                "trade_date": now.date().isoformat(),
                "snapshot_at": live.get("snapshot_at") or now.strftime("%Y-%m-%d %H:%M:%S"),
            }

    target = _portfolio_close_trade_date(now)
    bar = _portfolio_kline_bar(code, target, klines)
    if bar and bar.get("close") is not None:
        return {
            "mode": "post_close",
            "mode_label": "盘后收盘",
            "price": float(bar["close"]),
            "change_pct": float(bar.get("change_pct") or 0),
            "trade_date": str(bar.get("trade_date") or target)[:10],
            "turnover_ratio": bar.get("turnover_ratio"),
        }

    try:
        live = _portfolio_fetch_live_quotes([code]).get(code)
        if live and live.get("price") is not None:
            return {
                "mode": "post_close",
                "mode_label": "盘后收盘(行情补拉)",
                "price": float(live["price"]),
                "change_pct": float(live.get("change_pct") or 0),
                "trade_date": target,
                "snapshot_at": live.get("snapshot_at"),
            }
    except Exception as exc:
        _record_fallback('_portfolio_resolve_quote:5703', exc)

    if klines and klines[0].get("close") is not None:
        return {
            "mode": "post_close",
            "mode_label": "盘后收盘(库内最近)",
            "price": float(klines[0]["close"]),
            "change_pct": float(klines[0].get("change_pct") or 0),
            "trade_date": str(klines[0].get("trade_date", ""))[:10],
            "turnover_ratio": klines[0].get("turnover_ratio"),
        }
    return {"mode": mode, "mode_label": "数据不可用", "price": None, "change_pct": None, "trade_date": target}


def _portfolio_fetch_live_quotes(
    codes: list[str],
    *,
    force: bool = False,
    allow_remote: bool = False,
) -> dict[str, dict]:
    """Read persisted quotes, optionally refreshing the selected quote source.

    Normal portfolio reads remain independent of any remote quote process.
    Explicit refresh actions run in a background thread.
    """
    _log = logging.getLogger("portfolio.live_quotes")

    clean = _safe_portfolio_stock_codes(codes)
    if not clean:
        return {}
    _cache_key = f"portfolio_live_quotes_{','.join(clean)}"
    if force:
        _cache_drop(_cache_key)
    cached = None if force else _cache_get(_cache_key, ttl_seconds=_trading_live_ttl_seconds(60, intraday_seconds=1))
    if cached is not None:
        return cached
    out = {} if force else _live_quotes_from_current_table(clean, max_age_seconds=PORTFOLIO_LIVE_FRESH_SECONDS)
    missing_codes = clean if force else [code for code in clean if code not in out]
    if allow_remote and missing_codes and (_is_monitor_trading_time() or force):
        try:
            from integrations.registry import resolve_source
            from tools.sync_market_realtime import sync_market_realtime

            if resolve_source("current") == "bigqmt":
                from tools.run_big_qmt_bridge import sync_big_qmt_realtime

                try:
                    sync_big_qmt_realtime(engine=get_current_engine(), codes=missing_codes)
                except Exception as qmt_exc:
                    _log.warning(
                        "Selected Big QMT quote refresh failed for %s codes; falling back to Sina: %s",
                        len(missing_codes),
                        qmt_exc,
                    )
                    sync_market_realtime(
                        engine=get_current_engine(),
                        codes=missing_codes,
                        source="sina",
                        archive_snapshot=False,
                        run_rt_ddl=False,
                        skip_closed=False,
                        min_coverage=0.0,
                        replace_scope="subset",
                    )
            else:
                sync_market_realtime(
                    engine=get_current_engine(),
                    codes=missing_codes,
                    source="sina",
                    archive_snapshot=False,
                    run_rt_ddl=False,
                    skip_closed=False,
                    min_coverage=0.0,
                    replace_scope="subset",
                )
            out.update(_live_quotes_from_current_table(missing_codes, max_age_seconds=PORTFOLIO_LIVE_FRESH_SECONDS))
        except Exception as exc:
            _log.warning("Selected live quote refresh failed for %s codes: %s", len(missing_codes), exc)
    still_missing = [code for code in clean if code not in out]
    if still_missing:
        out.update(
            _live_quotes_from_current_table(
                still_missing,
                max_age_seconds=PORTFOLIO_LIVE_FRESH_SECONDS,
                allow_stale=True,
                max_stale_age_seconds=PORTFOLIO_LIVE_STALE_SECONDS,
            )
        )
    _cache_set(_cache_key, out)
    return out


def _queue_portfolio_qmt_refresh(codes: list[str]) -> dict[str, object]:
    """Queue a public-source refresh without holding an API worker open."""
    global _portfolio_qmt_refresh_thread, _portfolio_qmt_refresh_state

    clean = _safe_portfolio_stock_codes(codes)
    if not clean:
        return {"state": "idle", "requested": 0, "refreshed": 0, "stale": 0, "missing": 0}

    with _portfolio_qmt_refresh_lock:
        if _portfolio_qmt_refresh_thread is not None and _portfolio_qmt_refresh_thread.is_alive():
            return dict(_portfolio_qmt_refresh_state)

        started_at = datetime.now().isoformat(timespec="seconds")
        _portfolio_qmt_refresh_state = {
            "state": "queued",
            "started_at": started_at,
            "finished_at": "",
            "requested": len(clean),
            "refreshed": 0,
            "stale": 0,
            "missing": len(clean),
            "error": "",
        }

        def _run() -> None:
            global _portfolio_qmt_refresh_state
            try:
                quotes = _portfolio_fetch_live_quotes(clean, force=True, allow_remote=True)
                fresh_count = sum(
                    1 for item in quotes.values() if str(item.get("quote_status") or "") == "fresh"
                )
                stale_count = sum(
                    1 for item in quotes.values() if str(item.get("quote_status") or "") == "stale"
                )
                missing_count = max(0, len(clean) - fresh_count - stale_count)
                complete = fresh_count == len(clean)
                coverage_error = "" if complete else (
                    f"fresh quote coverage {fresh_count}/{len(clean)}; "
                    f"stale={stale_count}; missing={missing_count}"
                )
                _invalidate_portfolio_snapshot_cache()
                with _portfolio_qmt_refresh_lock:
                    _portfolio_qmt_refresh_state = {
                        **_portfolio_qmt_refresh_state,
                        "state": "success" if complete else "degraded",
                        "finished_at": datetime.now().isoformat(timespec="seconds"),
                        "requested": len(clean),
                        "refreshed": fresh_count,
                        "stale": stale_count,
                        "missing": missing_count,
                        "error": coverage_error,
                    }
            except Exception as exc:
                logging.getLogger("portfolio.refresh_prices").warning(
                    "background public quote refresh failed for %s codes: %s", len(clean), exc
                )
                with _portfolio_qmt_refresh_lock:
                    _portfolio_qmt_refresh_state = {
                        **_portfolio_qmt_refresh_state,
                        "state": "failed",
                        "finished_at": datetime.now().isoformat(timespec="seconds"),
                        "error": str(exc)[:200],
                    }

        _portfolio_qmt_refresh_thread = _threading.Thread(
            target=_run,
            name="portfolio-public-quote-refresh",
            daemon=True,
        )
        _portfolio_qmt_refresh_thread.start()
        return dict(_portfolio_qmt_refresh_state)


@router.get("/portfolio/live")
def portfolio_live(force: bool = False, refresh_id: str = ""):
    """自选股实时行情：拉取最新价 + 返回自选股数据（含当日盈亏/持仓盈亏），一次搞定"""
    if force:
        if refresh_id:
            return _get_portfolio_snapshot(
                live_mode=True,
                force_live=True,
                force_request_id=refresh_id,
            )
        return _get_portfolio_snapshot(live_mode=True, force_live=True)
    return _get_portfolio_snapshot(live_mode=True)


@router.post("/portfolio/refresh-prices")
def portfolio_refresh_prices():
    """异步刷新自选股实时行情：拉取最新价写入 sm_stock_current"""
    _log = logging.getLogger("portfolio.refresh_prices")
    try:
        pf_rows = _read_sql("SELECT stock_code FROM st_user_portfolio ORDER BY sort_order")
        codes = [r["stock_code"] for r in pf_rows] if pf_rows else []
        _log.info("刷新行情: %d 只自选股, codes=%s", len(codes), codes[:5])
        if not codes:
            return {"status": "ok", "refreshed": 0, "message": "无自选股"}
        state = _queue_portfolio_qmt_refresh(codes)
        return {
            "status": "ok",
            "state": state.get("state", "queued"),
            "requested": int(state.get("requested") or len(codes)),
            "refreshed": int(state.get("refreshed") or 0),
            "stale": int(state.get("stale") or 0),
            "missing": int(state.get("missing") or 0),
            "message": "行情同步已转后台执行，页面将继续使用已落库行情。",
        }
    except ImportError:
        _log.error("ImportError: adata 模块不可用")
        return {"status": "error", "error": "adata 模块不可用，无法获取实时行情"}
    except Exception as e:
        _log.error("刷新行情异常: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)[:200]}


@router.get("/portfolio/refresh-prices/status")
def portfolio_refresh_prices_status():
    """Return the state of the asynchronous portfolio quote refresh."""
    with _portfolio_qmt_refresh_lock:
        return {"status": "ok", "state": dict(_portfolio_qmt_refresh_state)}


@router.get("/portfolio/analyze/{stock_code}")
def portfolio_analyze(stock_code: str):
    """AI 分析个股 — DeepSeek 生成详细文字分析"""
    try:
        code = stock_code.strip().zfill(6)
        mode = _portfolio_market_mode()
        payload = _load_stock_detail_payload(code, mode=mode, light=True)
        basic = payload.get("basic") or {}
        market = payload.get("market") or {}
        capital = payload.get("capital") or {}
        finance = payload.get("finance") or {}
        valuation = payload.get("valuation") or {}
        technical = payload.get("technical") or {}
        industry = payload.get("industry") or ""
        concepts = payload.get("concepts") or []
        holding = payload.get("holding")
        trade_date = str(payload.get("trade_date") or "")
        requested_trade_date = str(payload.get("requested_trade_date") or trade_date or "")
        quote_trade_date = str(payload.get("quote_trade_date") or trade_date or "")
        flow_trade_date = str(payload.get("flow_trade_date") or quote_trade_date or "")
        price_val = float(market.get("price") or 0)
        quote = market
        analysis_snapshot = _load_latest_analysis_snapshot(code, trade_date=trade_date or None)
        recommendation_snapshot = _load_latest_recommendation_snapshot(code, trade_date=trade_date or None)

        ai_result = _generate_ai_analysis(
            code,
            basic.get("short_name"),
            market,
            capital,
            finance,
            valuation,
            technical,
            industry,
            concepts,
            holding,
            hot_rank=payload.get("hot_rank"),
            trade_date=trade_date,
            analysis_snapshot=analysis_snapshot,
            prefer_snapshot=True,
        )

        # 构建返回
        analysis_text = ai_result.get("conclusion", "") if isinstance(ai_result, dict) else str(ai_result)
        if not analysis_text:
            analysis_text = _build_portfolio_section_analysis(
                market=market,
                capital=capital,
                technical=technical,
                concepts=concepts,
                holding=holding,
                hot_rank=payload.get("hot_rank"),
                ai_result=ai_result if isinstance(ai_result, dict) else {},
                analysis_snapshot=analysis_snapshot,
                recommendation_snapshot=recommendation_snapshot,
            )
        data_mode_label = "盘中实时" if mode == "intraday" else "盘后收盘"

        response = {
            "stock_code": code,
            "short_name": basic.get("short_name"),
            "analysis": analysis_text,
            "analysis_time": datetime.now().strftime("%m月%d日 %H:%M"),
            "cur_price": price_val,
            "change_pct": quote.get("change_pct"),
            "data_mode": mode,
            "data_mode_label": data_mode_label,
            "quote_trade_date": quote_trade_date,
            "requested_trade_date": requested_trade_date,
            "flow_trade_date": flow_trade_date,
            "quote_source": payload.get("quote_source") or "",
            "detail_source": payload.get("detail_source") or "",
            "analysis_snapshot": analysis_snapshot,
            "recommendation_snapshot": recommendation_snapshot,
            "ai_source": ai_result.get("source") if isinstance(ai_result, dict) else None,
            "ai_analysis_date": ai_result.get("analysis_date") if isinstance(ai_result, dict) else None,
            "ai_scores": ai_result.get("scores") if isinstance(ai_result, dict) else None,
            "ai_score": ai_result.get("score") if isinstance(ai_result, dict) else None,
            "ai_action": ai_result.get("action") if isinstance(ai_result, dict) else None,
            "ai_action_reason": ai_result.get("action_reason") if isinstance(ai_result, dict) else None,
            "ai_recommend_status": ai_result.get("recommend_status") if isinstance(ai_result, dict) else None,
            "ai_event_risk_level": ai_result.get("event_risk_level") if isinstance(ai_result, dict) else None,
            "quote_is_stale": bool(requested_trade_date and quote_trade_date and quote_trade_date < requested_trade_date),
            "flow_is_stale": bool(requested_trade_date and flow_trade_date and flow_trade_date < requested_trade_date),
            "analysis_is_stale": bool(
                requested_trade_date
                and isinstance(ai_result, dict)
                and str(ai_result.get("analysis_date") or "") < requested_trade_date
            ),
        }

        # 持仓信息
        if holding:
            cost = float(holding.get("cost_price") or 0)
            shares = int(holding.get("shares") or 0)
            if price_val > 0 and cost > 0:
                profit_pct = round((price_val - cost) / cost * 100, 2)
                profit_amount = round((price_val - cost) * shares, 2)
                response["holding"] = {
                    "shares": shares,
                    "cost_price": cost,
                    "cur_price": price_val,
                    "profit_pct": profit_pct,
                    "profit_amount": profit_amount,
                }

        # 存历史记录
        try:
            _exec_sql("""
                INSERT INTO st_portfolio_analysis_log
                (stock_code, stock_name, analysis_text, cost_price, cur_price, change_pct, position_info, created_at)
                VALUES (:c, :n, :a, :cp, :pr, :chg, :pi, NOW())
            """, {"c": code, "n": basic.get("short_name"),
                   "a": analysis_text[:500] if analysis_text else "",
                   "cp": holding.get("cost_price") if holding else None,
                   "pr": price_val,
                   "chg": quote.get("change_pct"),
                   "pi": f"持仓{holding.get('shares')}股" if holding else "无持仓"})
        except Exception as exc:
            _record_fallback('portfolio_analyze:5930', exc)

        return response
    except Exception as e:
        return {"stock_code": stock_code, "error": f"分析异常: {e}"}


@router.get("/portfolio/analysis-history/{stock_code}")
def portfolio_analysis_history(stock_code: str):
    """查看某只股票的历史AI分析记录"""
    try:
        code = stock_code.strip().zfill(6)
        rows = _read_sql("""
            SELECT id, stock_code, stock_name, analysis_text, cost_price, cur_price,
                   change_pct, position_info,
                   DATE_FORMAT(created_at, '%m月%d日 %H:%i') AS analysis_time
            FROM st_portfolio_analysis_log
            WHERE stock_code = :c ORDER BY created_at DESC LIMIT 20
        """, {"c": code})
        return {"stock_code": code, "history": rows}
    except Exception as e:
        return {"stock_code": stock_code, "history": [], "error": str(e)}


# ═══════════════════════════════════════════
# 板块热度矩阵 API
# ═══════════════════════════════════════════

SECTOR_GROUPS = {
    "科技与自主可控": ["电子", "计算机", "传媒", "通信", "国防军工"],
    "复苏线": ["电力设备", "房地产", "建筑装饰", "银行", "非银金融", "医药生物",
              "家用电器", "食品饮料", "汽车", "美容护理", "商贸零售",
              "社会服务", "交通运输", "纺织服饰", "轻工制造", "机械设备"],
    "公共事业": ["公共事业", "环保"],
    "周期品": ["建筑材料", "基础化工", "煤炭", "石油石化", "有色金属", "钢铁", "农林牧渔"],
    "其他": ["综合"],
}

INDUSTRY_TO_GROUP = {}
for _g, _sl in SECTOR_GROUPS.items():
    for _s in _sl:
        INDUSTRY_TO_GROUP[_s] = _g

INDUSTRY_NAME_MAP = {
    "电力": "电力设备", "电力设备": "电力设备",
    "电网设备": "电力设备", "光伏设备": "电力设备",
    "半导体": "电子", "半导体设备": "电子", "消费电子": "电子", "芯片概念": "电子",
    "计算机应用": "计算机", "计算机设备": "计算机", "IT服务": "计算机",
    "文化传媒": "传媒", "广告营销": "传媒", "游戏": "传媒",
    "通信设备": "通信", "通信服务": "通信",
    "国防军工": "国防军工", "地面兵装": "国防军工", "航空装备": "国防军工", "航天装备": "国防军工",
    "建筑装饰": "建筑装饰", "房屋建设": "建筑装饰", "装修装饰": "建筑装饰",
    "银行": "银行",
    "证券": "非银金融", "保险": "非银金融", "多元金融": "非银金融",
    "医药生物": "医药生物", "化学制药": "医药生物", "生物制品": "医药生物",
    "家用电器": "家用电器",
    "食品饮料": "食品饮料", "白酒": "食品饮料", "食品加工": "食品饮料", "饮料乳品": "食品饮料",
    "汽车": "汽车", "汽车零部件": "汽车", "汽车服务": "汽车",
    "美容护理": "美容护理",
    "商贸零售": "商贸零售",
    "社会服务": "社会服务",
    "交通运输": "交通运输", "物流": "交通运输",
    "纺织服饰": "纺织服饰", "服装家纺": "纺织服饰",
    "轻工制造": "轻工制造", "造纸": "轻工制造", "包装印刷": "轻工制造",
    "机械设备": "机械设备", "通用设备": "机械设备", "专用设备": "机械设备", "自动化设备": "机械设备",
    "公共事业": "公共事业",
    "环保": "环保", "环境治理": "环保",
    "建筑材料": "建筑材料", "水泥": "建筑材料", "玻璃玻纤": "建筑材料",
    "基础化工": "基础化工", "化学制品": "基础化工",
    "煤炭": "煤炭", "煤化工": "煤炭",
    "石油石化": "石油石化", "油服工程": "石油石化", "石油开采": "石油石化",
    "有色金属": "有色金属", "能源金属": "有色金属", "工业金属": "有色金属",
    "钢铁": "钢铁", "冶钢原料": "钢铁",
    "农林牧渔": "农林牧渔", "养殖业": "农林牧渔", "种植业": "农林牧渔",
    "综合": "综合",
}

# 东财一级→二级行业映射
EAST_INDUSTRY_MAP = {
    "汽车": ["汽车服务", "汽车零部件", "乘用车", "摩托车及其他", "商用车"],
    "机械设备": ["自动化设备", "通用设备", "工程机械", "专用设备", "轨交设备"],
    "传媒": ["广告营销", "影视院线", "数字媒体", "游戏", "出版", "电视广播"],
    "家用电器": ["其他家电", "小家电", "家电零部件", "白色家电", "厨卫电器", "黑色家电", "照明设备"],
    "煤炭": ["煤炭开采", "焦炭"],
    "基础化工": ["化学原料", "化学制品", "橡胶", "塑料", "化学纤维", "农化制品", "非金属材料"],
    "轻工制造": ["文娱用品", "家居用品", "包装印刷", "造纸"],
    "电子": ["电子化学品", "半导体", "消费电子", "其他电子", "光学光电子", "元件"],
    "社会服务": ["体育", "旅游及景区", "专业服务", "教育", "酒店餐饮"],
    "石油石化": ["油服工程", "油气开采", "炼化及贸易"],
    "纺织服饰": ["服装家纺", "纺织制造", "饰品"],
    "计算机": ["软件开发", "IT服务", "计算机设备"],
    "建筑装饰": ["工程咨询服务", "装修装饰", "专业工程", "房屋建设", "基础建设"],
    "医药生物": ["医疗服务", "化学制药", "中药", "医疗器械", "生物制品", "医药商业"],
    "银行": ["银行"],
    "电力设备": ["电机", "电池", "其他电源设备", "风电设备", "电网设备", "光伏设备"],
    "环保": ["环保设备", "环境治理"],
    "美容护理": ["化妆品", "医疗美容", "个护用品"],
    "房地产": ["房地产开发", "房地产服务"],
    "公用事业": ["燃气", "电力"],
    "交通运输": ["航运港口", "航空机场", "物流", "铁路公路"],
    "综合": ["综合"],
    "通信": ["通信服务", "通信设备"],
    "农林牧渔": ["饲料", "动物保健", "渔业", "林业", "农产品加工", "农业综合", "养殖业", "种植业"],
    "食品饮料": ["休闲食品", "非白酒", "饮料乳品", "调味发酵品", "食品加工", "白酒"],
    "国防军工": ["航海装备", "地面兵装", "航空装备", "军工电子", "航天装备"],
    "商贸零售": ["专业连锁", "互联网电商", "贸易", "一般零售", "旅游零售"],
    "钢铁": ["特钢", "冶钢原料", "普钢"],
    "建筑材料": ["装修建材", "水泥", "玻璃玻纤"],
    "非银金融": ["证券", "保险", "多元金融"],
    "有色金属": ["金属新材料", "能源金属", "小金属", "工业金属", "贵金属"],
}


@router.get("/hot-data/sector-heat-matrix")
def sector_heat_matrix(
    end_date: str = Query(default_factory=lambda: date.today().isoformat()),
    days: int = Query(default=26, ge=1, le=90),
    raw: bool = Query(default=False, description="true=同花顺原始行业名，不分组"),
):
    """板块热度矩阵 - 分两组：行业板块 + 概念板块"""
    from datetime import timedelta
    from collections import defaultdict

    try:
        requested_end_date = end_date
        try:
            fallback_rows = _read_sql(
                """
                SELECT MAX(snapshot_date) AS d
                FROM st_hot_concept_ths_daily
                WHERE snapshot_date <= :d AND plate_type IN (1,2,3,4)
                """,
                {"d": end_date},
            )
            fallback_date = str(fallback_rows[0]["d"])[:10] if fallback_rows and fallback_rows[0].get("d") else ""
            if fallback_date:
                end_date = fallback_date
        except Exception:
            fallback_date = ""
        end_dt = date.fromisoformat(end_date)
        begin = end_dt - timedelta(days=days * 2 + 10)

        today_str = date.today().isoformat()
        all_plate2_rows = []
        all_plate1_rows = []

        try:
            plate2 = _read_sql(
                "SELECT snapshot_date, concept_name, hot_value, plate_type FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date >= :b AND snapshot_date <= :e AND plate_type = 2 "
                "ORDER BY snapshot_date DESC",
                {"b": begin.isoformat(), "e": end_dt.isoformat()},
            )
            all_plate2_rows.extend(plate2)
        except Exception as exc:
            _record_fallback('sector_heat_matrix:6085', exc)

        try:
            plate1 = _read_sql(
                "SELECT snapshot_date, concept_name, hot_value, plate_type FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date >= :b AND snapshot_date <= :e AND plate_type = 1 "
                "ORDER BY snapshot_date DESC",
                {"b": begin.isoformat(), "e": end_dt.isoformat()},
            )
            all_plate1_rows.extend(plate1)
        except Exception as exc:
            _record_fallback('sector_heat_matrix:6096', exc)

        if not any(str(r.get("snapshot_date", "")) == today_str for r in all_plate2_rows):
            pass  # 由定时任务同步

        if not any(str(r.get("snapshot_date", "")) == today_str for r in all_plate1_rows):
            pass  # 由定时任务同步

        # East Money 东财行业(m:90+t:2) - 按映射表分级
        all_plate3_rows = []
        all_plate4_rows = []
        try:
            plate3 = _read_sql(
                "SELECT snapshot_date, concept_name, hot_value, plate_type FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date >= :b AND snapshot_date <= :e AND plate_type = 3 "
                "ORDER BY snapshot_date DESC",
                {"b": begin.isoformat(), "e": end_dt.isoformat()},
            )
            all_plate3_rows.extend(plate3)
            plate4 = _read_sql(
                "SELECT snapshot_date, concept_name, hot_value, plate_type FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date >= :b AND snapshot_date <= :e AND plate_type = 4 "
                "ORDER BY snapshot_date DESC",
                {"b": begin.isoformat(), "e": end_dt.isoformat()},
            )
            all_plate4_rows.extend(plate4)
        except Exception as exc:
            _record_fallback('sector_heat_matrix:6123', exc)

        groups = {}
        data = []
        raw_data = {}

        def _ensure_date_row(date_str: str) -> dict:
            for item in data:
                if item["date"] == date_str:
                    return item
            item = {
                "date": date_str,
                "ths_industry": {},
                "ths_concept": {},
                "east_industry": {},
                "east_industry_sub": {},
            }
            data.append(item)
            return item

        if all_plate2_rows:
            d2 = defaultdict(dict)
            for r in all_plate2_rows:
                d = str(r["snapshot_date"])
                name = str(r["concept_name"] or "").strip()
                val = float(r["hot_value"] or 0)
                d2[d][name] = max(d2[d].get(name, 0), val)

            dates2 = sorted(d2.keys(), reverse=True)[:days]
            names2 = sorted(set(n for dd in dates2 for n in d2[dd]))
            groups["同花顺行业TOP20"] = names2

            for date_str in dates2:
                row = _ensure_date_row(date_str)
                for n, val in d2[date_str].items():
                    row["ths_industry"][n] = round(val, 1)

        if all_plate1_rows:
            d1 = defaultdict(dict)
            for r in all_plate1_rows:
                d = str(r["snapshot_date"])
                name = str(r["concept_name"] or "").strip()
                val = float(r["hot_value"] or 0)
                d1[d][name] = max(d1[d].get(name, 0), val)

            dates1 = sorted(d1.keys(), reverse=True)[:days]
            names1 = sorted(set(n for dd in dates1 for n in d1[dd]))
            groups["同花顺概念板块TOP100"] = names1

            for date_str in dates1:
                row = _ensure_date_row(date_str)
                for n, val in d1[date_str].items():
                    row["ths_concept"][n] = round(val, 1)

        if all_plate3_rows:
            d3 = defaultdict(dict)
            for r in all_plate3_rows:
                d = str(r["snapshot_date"])
                name = str(r["concept_name"] or "").strip()
                val = float(r["hot_value"] or 0)
                d3[d][name] = max(d3[d].get(name, 0), val)
            dates3 = sorted(d3.keys(), reverse=True)[:days]
            names3 = [name for name in EAST_INDUSTRY_MAP if any(name in d3[dd] for dd in dates3)]
            groups["东财一级行业"] = names3
            for date_str in dates3:
                row = _ensure_date_row(date_str)
                for n, val in d3[date_str].items():
                    if n in EAST_INDUSTRY_MAP:
                        row["east_industry"][n] = round(val, 1)

        if all_plate4_rows:
            d4 = defaultdict(dict)
            for r in all_plate4_rows:
                d = str(r["snapshot_date"])
                name = str(r["concept_name"] or "").strip()
                val = float(r["hot_value"] or 0)
                d4[d][name] = max(d4[d].get(name, 0), val)
            dates4 = sorted(d4.keys(), reverse=True)[:days]
            fixed_names4 = [child for children in EAST_INDUSTRY_MAP.values() for child in children]
            names4 = [name for name in fixed_names4 if any(name in d4[dd] for dd in dates4)]
            groups["东财二级行业"] = names4
            for date_str in dates4:
                row = _ensure_date_row(date_str)
                for n, val in d4[date_str].items():
                    if n in fixed_names4:
                        row["east_industry_sub"][n] = round(val, 1)

        data.sort(key=lambda x: x["date"], reverse=True)

        # 只保留真正的交易日（从 si_trade_calendar 查询）
        data = [r for r in data if any(isinstance(r.get(k), dict) and len(r[k]) > 0 for k in ("ths_industry", "ths_concept", "east_industry", "east_industry_sub"))]
        if data:
            all_dates_in_data = sorted({r["date"] for r in data}, reverse=True)
            try:
                trading_dates = _read_sql(
                    "SELECT trade_date FROM si_trade_calendar WHERE trade_date IN :dates AND trade_status = 1",
                    {"dates": tuple(all_dates_in_data)}
                )
                trading_set = {str(r["trade_date"]) for r in trading_dates}
                data = [r for r in data if r["date"] in trading_set]
            except Exception:
                data = [r for r in data if date.fromisoformat(r["date"]).isoweekday() <= 5]

        # Merge rows with same date
        merged = {}
        for row in data:
            d = row["date"]
            if d not in merged:
                merged[d] = {"date": d}
            for k in ("ths_industry", "ths_concept", "east_industry", "east_industry_sub"):
                if k in row:
                    if k not in merged[d]:
                        merged[d][k] = {}
                    merged[d][k].update(row[k])
        data = list(merged.values())
        data.sort(key=lambda x: x["date"], reverse=True)

        # 热度值归一化 → 0~100
        # 所有数据源(东财成交额/THS搜索热度)都用全局log压缩+全局min-max归一化
        # 不同数据源独立归一化，各自内部跨日可比
        import math as _math

        # 1) 先保存原始值到 raw_data
        for row in data:
            d = row["date"]
            raw_data[d] = {}
            for key in ("ths_industry", "ths_concept", "east_industry", "east_industry_sub"):
                obj = row.get(key)
                if obj:
                    raw_data[d][key] = dict(obj)

        # 2) 对每组数据独立做全局log压缩 + 全局min-max归一化
        normalize_keys = ("ths_industry", "ths_concept", "east_industry", "east_industry_sub")
        for key in normalize_keys:
            # 收集该组所有日期的值
            all_log_vals = []
            for row in data:
                obj = row.get(key)
                if obj and len(obj) > 0:
                    for v in obj.values():
                        fv = float(v)
                        if fv > 0:
                            all_log_vals.append(_math.log(fv + 1))

            if not all_log_vals:
                continue

            g_min = min(all_log_vals)
            g_max = max(all_log_vals)
            g_range = g_max - g_min

            # 归一化
            for row in data:
                obj = row.get(key)
                if obj and len(obj) > 0:
                    if g_range > 0:
                        for name in list(obj.keys()):
                            fv = float(obj[name])
                            log_v = _math.log(fv + 1) if fv > 0 else g_min
                            obj[name] = round((log_v - g_min) / g_range * 100, 1)
                    else:
                        for name in obj:
                            obj[name] = 50.0

        # 3) 每日热度汇总（各组平均值，更直观）
        daily_totals = {}
        for row in data:
            d = row["date"]
            daily_totals[d] = {}
            for key in normalize_keys:
                obj = row.get(key)
                if obj and len(obj) > 0:
                    vals = [float(v) for v in obj.values()]
                    daily_totals[d][key] = round(sum(vals) / len(vals), 1)
                else:
                    daily_totals[d][key] = 0

        # Build east_tree with the fixed Eastmoney App level-1/level-2 header order.
        east_tree = {}
        if EAST_INDUSTRY_MAP and (all_plate3_rows or all_plate4_rows):
            visible_l1 = groups.get("东财一级行业") or list(EAST_INDUSTRY_MAP.keys())
            for l1 in visible_l1:
                children = EAST_INDUSTRY_MAP.get(l1)
                if children:
                    east_tree[l1] = list(children)

        warnings = []
        if not groups.get("东财一级行业"):
            warnings.append("东财一级行业热度缺失")
        if not groups.get("东财二级行业"):
            warnings.append("东财二级行业热度缺失")

        return {"date": end_date, "requested_date": requested_end_date, "fallback": end_date != requested_end_date,
                "dates": [r["date"] for r in data],
                "groups": groups, "data": data, "total": len(data), "east_tree": east_tree,
                "daily_totals": daily_totals, "raw_data": raw_data, "warnings": warnings}
    except Exception as e:
        return {"date": end_date, "data": [], "groups": {}, "total": 0, "error": str(e)}


@router.post("/hot-data/sector-heat-upload")
def sector_heat_upload(payload: list[dict]):
    """接收外部推送的板块热度数据（如东财行业数据）"""
    from sqlalchemy import text as _text
    try:
        if not payload:
            return {"status": "error", "message": "空数据"}
        engine3 = get_engine()
        with engine3.begin() as conn:
            for row in payload:
                sd = row.get("snapshot_date", str(date.today()))
                pt = row.get("plate_type", 3)
                rk = row.get("rank", 0)
                cc = str(row.get("concept_code", ""))[:32]
                cn = str(row.get("concept_name", ""))[:64]
                cp = row.get("change_pct")
                hv = float(row.get("hot_value", 0))
                conn.execute(_text(
                    "INSERT INTO st_hot_concept_ths_daily (snapshot_date,plate_type,`rank`,concept_code,concept_name,change_pct,hot_value,hot_tag,etl_sync_at) "
                    "VALUES (:d,:pt,:rk,:cc,:cn,:cp,:hv,'',NOW()) "
                    "ON DUPLICATE KEY UPDATE hot_value=VALUES(hot_value), change_pct=VALUES(change_pct)"
                ), {"d": sd, "pt": pt, "rk": rk, "cc": cc, "cn": cn, "cp": float(cp) if cp else None, "hv": hv})
        return {"status": "ok", "message": f"写入 {len(payload)} 条"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/hot-data/sector-heat-matrix/sync-today")
def sync_sector_heat_today(date: str = Query(default_factory=lambda: date.today().isoformat())):
    """触发东财板块热度同步（plate_type=3/4）"""
    import subprocess
    import sys as _sys
    import os as _os
    import re as _re
    _ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    cmd = [_sys.executable, str(_ROOT + "/tools/fetch_sector_heat_east_daily.py"), date]
    child_env = build_child_env(
        _ROOT,
        engine=get_engine(),
        override_mysql_url=False,
    )
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=_ROOT, env=child_env)
        output = ((r.stdout or "") + ("\n" + r.stderr if r.stderr else "")).strip()
        synced = 0
        m = _re.search(r"SYNCED=(\d+)", output)
        if m:
            synced = int(m.group(1))
        actual_date = date
        dm = _re.search(r"DATE=(\d{4}-\d{2}-\d{2})", output)
        if dm:
            actual_date = dm.group(1)
        return {
            "status": "success" if r.returncode == 0 else "failed",
            "date": actual_date,
            "requested_date": date,
            "synced": synced,
            "output": output[:1000],
            "error": (r.stderr or "").strip()[:500] if r.returncode != 0 else "",
        }
    except Exception as e:
        return {"status": "error", "date": date, "error": str(e)}


@router.get("/hot-data/market-sentiment")
def market_sentiment(
    days: int = Query(default=20, ge=5, le=60),
    date: str = Query(default_factory=lambda: date.today().isoformat()),
    top: int = Query(default=5, ge=1, le=15),
    include_signal: bool = Query(default=False),
):
    """市场情绪与风格分析 — 主线/轮动/大小盘"""
    try:
        result = _market_sentiment_result(date, days, top)
        if include_signal and "error" not in result:
            result = dict(result)
            result["style_switch_signal"] = _build_style_switch_signal(date, days, result)
        return result
    except Exception as e:
        return {"error": str(e)}


def _style_switch_signal_from_inputs(sentiment: dict, news_rows: list[dict]) -> dict:
    theme = sentiment.get("theme_analysis") or {}
    style = sentiment.get("style_analysis") or {}
    cap = sentiment.get("capital_analysis") or {}
    rotation_score = float(theme.get("rotation_score") or 0)
    risk_off_score = 20.0
    switch_score = min(100.0, rotation_score)
    evidence: list[str] = []

    phase_desc = str(theme.get("phase_desc") or "")
    style_bias = str(style.get("bias") or "")
    style_desc = str(style.get("bias_desc") or "")
    flow_style = str(cap.get("flow_style") or "")
    recent_trend = str(cap.get("recent_trend") or "")

    if rotation_score >= 60:
        switch_score += 18
        evidence.append(f"轮动强度{rotation_score:.0f}偏高")
    elif rotation_score >= 40:
        switch_score += 8
        evidence.append(f"轮动强度{rotation_score:.0f}中等")
    if "轮动" in str(theme.get("phase") or "") or "切换" in phase_desc:
        switch_score += 12
        evidence.append("主线阶段提示轮动/切换")

    if "大盘" in style_bias or "核心资产" in style_desc or "避险" in style_desc:
        risk_off_score += 22
        evidence.append("风格偏向大盘/核心资产")
    if "流出" in flow_style or "流出" in recent_trend:
        risk_off_score += 20
        evidence.append("主力资金偏流出")

    risk_keywords = ["监管", "制裁", "加息", "冲突", "战争", "暴跌", "违约", "退市", "关税", "汇率"]
    policy_keywords = ["降准", "降息", "稳增长", "财政", "专项债", "国常会", "政策", "会议"]
    tech_keywords = ["芯片", "半导体", "AI", "人工智能", "算力", "机器人", "新能源"]
    risk_news = 0
    policy_news = 0
    tech_news = 0
    hot_subjects: dict[str, int] = {}
    for row in news_rows[:80]:
        text_blob = str(row.get("title") or "") + str(row.get("content") or "")
        if any(kw in text_blob for kw in risk_keywords):
            risk_news += 1
        if any(kw in text_blob for kw in policy_keywords):
            policy_news += 1
        if any(kw in text_blob for kw in tech_keywords):
            tech_news += 1
        for item in row.get("subjects") or []:
            name = item.get("name") if isinstance(item, dict) else str(item)
            if name:
                hot_subjects[name] = hot_subjects.get(name, 0) + 1

    if risk_news >= 2:
        risk_off_score += min(24, risk_news * 6)
        evidence.append(f"风险/监管/外部扰动新闻{risk_news}条")
    if policy_news >= 2:
        switch_score += min(16, policy_news * 4)
        evidence.append(f"政策类新闻{policy_news}条，可能触发方向切换")
    if tech_news >= 3 and risk_news >= 2:
        switch_score += 8
        evidence.append("科技热度高但风险新闻增多，警惕高弹性板块分歧")

    tech_risk_signal = build_tech_risk_signal(news_rows, [])
    if tech_risk_signal.get("triggered"):
        risk_off_score += min(28, float(tech_risk_signal.get("score") or 0) * 0.28)
        switch_score += 10
        evidence.append(str(tech_risk_signal.get("headline") or "事件/板块风险触发"))

    risk_off_score = round(max(0, min(100, risk_off_score)), 1)
    switch_score = round(max(0, min(100, switch_score)), 1)
    status = "risk_off" if risk_off_score >= 65 else "switching" if switch_score >= 65 else "balanced"
    if status == "risk_off":
        summary = "风险偏好下降，优先控制回撤，关注防御/避险方向"
        action = "降低高波动、高拥挤仓位；优先观察银行、公用事业、电力、煤炭、石油石化、贵金属等防御线。"
    elif status == "switching":
        summary = "板块切换概率升高，注意主线从旧热点向政策/低位方向轮动"
        action = "旧主线冲高不追，跟踪资金新进板块和首批放量个股，先小仓试错。"
    else:
        summary = "暂未出现强切换信号，维持主线跟踪"
        action = "保持仓位纪律，等资金和情绪方向进一步确认。"

    top_subjects = sorted(hot_subjects.items(), key=lambda kv: kv[1], reverse=True)[:8]
    return {
        "status": status,
        "summary": summary,
        "action": action,
        "risk_off_score": risk_off_score,
        "switch_score": switch_score,
        "defensive_sectors": ["银行", "公用事业", "电力", "煤炭", "石油石化", "贵金属"],
        "evidence": evidence[:8],
        "news_counts": {"risk": risk_news, "policy": policy_news, "tech": tech_news},
        "hot_subjects": [{"name": name, "count": count} for name, count in top_subjects],
        "tech_risk_signal": tech_risk_signal,
        "decision_radar": tech_risk_signal,
    }


def _market_sentiment_news_rows() -> list[dict]:
    news_rows = _read_sql(
        """
        SELECT title, content, publish_time, subjects
        FROM st_news_flash
        WHERE publish_time >= DATE_SUB(NOW(), INTERVAL 36 HOUR)
        ORDER BY publish_time DESC
        LIMIT 120
        """
    )
    for row in news_rows:
        try:
            row["subjects"] = json.loads(row.get("subjects") or "[]")
        except Exception:
            row["subjects"] = []
    return news_rows


def _build_style_switch_signal(date_value: str, days: int, sentiment: dict | None = None) -> dict:
    if sentiment is None:
        sentiment = _market_sentiment_result(date_value, days, 8)
    news_rows = _market_sentiment_news_rows()
    signal = _style_switch_signal_from_inputs(sentiment if isinstance(sentiment, dict) else {}, news_rows)
    try:
        signal["tech_risk_signal"] = fetch_tech_risk_signal(_read_sql, date_value)
        signal["decision_radar"] = signal["tech_risk_signal"]
    except Exception as exc:
        _record_fallback('_build_style_switch_signal:6530', exc)
    return {
        "date": date_value,
        "sentiment_date": sentiment.get("latest_date") if isinstance(sentiment, dict) else "",
        **signal,
    }


@router.get("/hot-data/style-switch-signal")
def style_switch_signal(
    date: str = Query(default_factory=lambda: date.today().isoformat()),
    days: int = Query(default=20, ge=5, le=60),
):
    try:
        return _build_style_switch_signal(date, days)
    except Exception as e:
        return {"date": date, "error": str(e)}


@router.get("/hot-data/tech-risk-signal")
def tech_risk_signal(
    date: str = Query(default_factory=lambda: date.today().isoformat()),
    days: int = Query(default=2, ge=1, le=7),
):
    """兼容旧入口：当前返回通用风险/机会决策雷达。"""
    try:
        return fetch_tech_risk_signal(_read_sql, date, days=days)
    except Exception as e:
        return {"date": date, "status": "error", "triggered": False, "error": str(e)[:200]}


@router.get("/hot-data/decision-radar")
def decision_radar(
    date: str = Query(default_factory=lambda: date.today().isoformat()),
    days: int = Query(default=2, ge=1, le=7),
):
    """风险/机会决策雷达：新闻 + 外围/A股盘面 + 持仓/推荐池。"""
    try:
        return fetch_tech_risk_signal(_read_sql, date, days=days)
    except Exception as e:
        return {"date": date, "status": "error", "triggered": False, "error": str(e)[:200]}


def _exec_sql(sql: str, params: dict = None):
    from server.api.routers._engine import get_engine as _ge
    e = _ge()
    with e.begin() as c:
        c.execute(text(sql), params)


def _read_dotenv_key():
    return (get_settings().deepseek_api_key or "").strip()


def _is_monitor_trading_time():
    from datetime import datetime as _dt
    now = _dt.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    return (925 <= t <= 1135) or (1255 <= t <= 1505)


def _market_live_cache_ttl() -> int:
    """Keep full-market views reasonably fresh without pretending they are tick-by-tick."""
    return 10 if _is_monitor_trading_time() else 120


def _trading_live_ttl_seconds(default_off_hours: int = 60, *, intraday_seconds: int = 0) -> int:
    return intraday_seconds if _is_monitor_trading_time() else default_off_hours


def _live_quotes_from_current_table(
    codes: list[str],
    *,
    max_age_seconds: int = 20,
    allow_stale: bool = False,
    max_stale_age_seconds: int = 300,
) -> dict[str, dict]:
    clean = _safe_portfolio_stock_codes(codes)
    if not clean:
        return {}
    placeholders = ", ".join([f":code_{idx}" for idx, _ in enumerate(clean)])
    params = {f"code_{idx}": code for idx, code in enumerate(clean)}
    rows = _read_sql(
        f"""
        SELECT stock_code, short_name, price, `change`, change_pct, volume, amount, snapshot_at
        FROM sm_stock_current
        WHERE stock_code IN ({placeholders})
        """,
        params,
    )
    if not rows:
        return {}
    out: dict[str, dict] = {}
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    for row in rows:
        stock_code = str(row.get("stock_code") or "").strip().zfill(6)
        snapshot_at = str(row.get("snapshot_at") or "")
        if not stock_code or not snapshot_at.startswith(today):
            continue
        try:
            age_seconds = (now - datetime.strptime(snapshot_at[:19], "%Y-%m-%d %H:%M:%S")).total_seconds()
        except Exception:
            age_seconds = float(max_age_seconds + 1)
        if age_seconds > max_age_seconds:
            if not allow_stale or age_seconds > max_stale_age_seconds:
                continue
            quote_status = "stale"
        else:
            quote_status = "fresh"
        out[stock_code] = {
            "stock_code": stock_code,
            "short_name": row.get("short_name"),
            "price": row.get("price"),
            "change": row.get("change"),
            "change_pct": row.get("change_pct"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "snapshot_at": snapshot_at,
            "source": "qmt_live_table" if quote_status == "fresh" else "qmt_live_table_stale",
            "quote_status": quote_status,
            "quote_age_seconds": int(max(0, age_seconds)),
        }
    return out


def _portfolio_closed_quotes_from_current_table(codes: list[str], trade_date: str) -> dict[str, dict]:
    """Return stable close/off-hours quotes from the latest stored quote snapshot for a trade date."""
    clean = _safe_portfolio_stock_codes(codes)
    td = str(trade_date or "")[:10]
    out: dict[str, dict] = {}
    if not clean or not td:
        return out
    try:
        from datetime import datetime as _dt, timedelta as _td

        td_start = _dt.strptime(td, "%Y-%m-%d")
        td_end = td_start + _td(days=1)
    except Exception:
        return out
    placeholders = ", ".join([f":code_{idx}" for idx, _ in enumerate(clean)])
    params = {f"code_{idx}": code for idx, code in enumerate(clean)}
    params["td"] = td
    params["td_start"] = td_start.strftime("%Y-%m-%d %H:%M:%S")
    params["td_end"] = td_end.strftime("%Y-%m-%d %H:%M:%S")

    def _coerce_rows(rows: list[dict], source: str) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for row in rows or []:
            stock_code = str(row.get("stock_code") or "").strip().zfill(6)
            if not stock_code or row.get("price") is None:
                continue
            snapshot_at = str(row.get("snapshot_at") or "")
            out[stock_code] = {
                "stock_code": stock_code,
                "short_name": row.get("short_name"),
                "price": row.get("price"),
                "change": row.get("change"),
                "change_pct": row.get("change_pct"),
                "volume": row.get("volume"),
                "amount": row.get("amount"),
                "snapshot_at": snapshot_at,
                "source": source,
                "quote_status": "closed",
                "quote_age_seconds": None,
            }
        return out

    try:
        rows = _read_sql(
            f"""
            SELECT stock_code, short_name, price, `change`, change_pct, volume, amount, snapshot_at
            FROM sm_stock_current
            WHERE stock_code IN ({placeholders})
              AND snapshot_at >= :td_start AND snapshot_at < :td_end
            """,
            params,
        )
        out = _coerce_rows(rows, "current_close_table")
        if len(out) >= len(clean):
            return out
    except Exception as exc:
        _record_fallback('_portfolio_closed_quotes_from_current_table:6704', exc)

    try:
        rows = _read_sql(
            f"""
            SELECT q.stock_code, q.short_name, q.price, q.`change`, q.change_pct,
                   q.volume, q.amount, q.snapshot_at
            FROM sm_rt_quote_snapshot q
            INNER JOIN (
                SELECT stock_code, MAX(snapshot_at) AS snapshot_at
                FROM sm_rt_quote_snapshot
                WHERE stock_code IN ({placeholders})
                  AND snapshot_at >= :td_start AND snapshot_at < :td_end
                GROUP BY stock_code
            ) x ON q.stock_code = x.stock_code AND q.snapshot_at = x.snapshot_at
            """,
            params,
        )
        archived = _coerce_rows(rows, "current_close_archive")
        return {**archived, **out}
    except Exception:
        return out


def _get_realtime_overview(*, allow_close: bool = False):
    aggregate_columns = (
        "SUM(CASE WHEN q.change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt, "
        "SUM(CASE WHEN q.change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt, "
        "SUM(CASE WHEN ABS(q.change_pct) < 1 THEN 1 ELSE 0 END) AS sideline_cnt, "
        "COUNT(*) AS total, COALESCE(SUM(q.amount), 0) AS total_amount, "
        "SUM(CASE WHEN (q.stock_code LIKE '002%%' OR q.stock_code LIKE '300%%' OR q.stock_code LIKE '301%%') "
        "AND q.change_pct > 0 THEN 1 ELSE 0 END) AS small_up_cnt, "
        "SUM(CASE WHEN (q.stock_code LIKE '002%%' OR q.stock_code LIKE '300%%' OR q.stock_code LIKE '301%%') "
        "THEN 1 ELSE 0 END) AS small_total, "
        "AVG(CASE WHEN (q.stock_code LIKE '002%%' OR q.stock_code LIKE '300%%' OR q.stock_code LIKE '301%%') "
        "THEN q.change_pct END) AS small_avg_chg, "
        "MAX(q.snapshot_at) AS data_time, "
        "SUM(CASE WHEN DATE(q.snapshot_at) = CURDATE() THEN 1 ELSE 0 END) AS today_count"
    )
    receipt = None
    try:
        receipt_sql = """
            SELECT expected_count, observed_count, coverage, published_at,
                   source_generated_at, capture_mode
            FROM st_qmt_realtime_sync_receipt_v2
            WHERE source_provider = 'gj_big_qmt_inner'
              AND quality_status = 'PASS'
              AND coverage >= 0.95
        """
        if allow_close:
            receipt_sql += """
              AND capture_mode IN ('LIVE_FORWARD', 'OFF_SESSION_SNAPSHOT')
              AND published_at >= CURDATE()
              AND source_generated_at >= CURDATE()
            """
        else:
            receipt_sql += """
              AND capture_mode = 'LIVE_FORWARD'
              AND published_at >= NOW() - INTERVAL 90 SECOND
              AND source_generated_at >= NOW() - INTERVAL 90 SECOND
            """
        receipt_sql += """
            ORDER BY published_at DESC, created_at DESC
            LIMIT 1
        """
        # The attestation is stored with the quote plane rather than the
        # business ledger, so do not route this query through ``_read_sql``.
        with get_current_engine().connect() as conn:
            receipt_row = conn.execute(text(receipt_sql)).mappings().first()
        receipt = dict(receipt_row) if receipt_row else None
    except Exception as exc:
        _record_fallback('_get_realtime_overview:receipt', exc)

    try:
        # The QMT bridge atomically replaces this table only after full-market
        # coverage passes validation, so it is the authoritative live view.
        rows = _read_sql(
            f"SELECT {aggregate_columns} FROM sm_stock_current q"
        )
        if rows:
            current = rows[0]
            total = int(current.get("total") or 0)
            today_count = int(current.get("today_count") or 0)
            observed = int((receipt or {}).get("observed_count") or 0)
            receipt_matches = bool(receipt and total >= max(3000, int(observed * 0.95)))
            session_rows_match = total >= 3000 and today_count >= int(total * 0.90)
            if receipt_matches and session_rows_match:
                if not allow_close:
                    current["data_time"] = (receipt or {}).get("published_at") or current.get("data_time")
                current["data_source"] = "sm_stock_current"
                current["coverage"] = float((receipt or {}).get("coverage") or 0)
                current["session_coverage"] = round(today_count / max(total, 1), 4)
                current["capture_mode"] = (receipt or {}).get("capture_mode") or ""
                return current
    except Exception as exc:
        _record_fallback('_get_realtime_overview:current', exc)

    if allow_close:
        # Paused/closed sessions must use the attested whole-market batch;
        # a rolling archive can contain newly written but stale cached quotes.
        return None

    try:
        # Archive snapshots are written in rolling batches.  Pick the newest
        # row per stock within the freshness window instead of requiring every
        # stock to share the exact same second.
        rows = _read_sql(
            f"""
            SELECT {aggregate_columns}
            FROM sm_rt_quote_snapshot q
            INNER JOIN (
                SELECT stock_code, MAX(snapshot_at) AS snapshot_at
                FROM sm_rt_quote_snapshot
                WHERE snapshot_at >= NOW() - INTERVAL 5 MINUTE
                GROUP BY stock_code
            ) latest
              ON q.stock_code = latest.stock_code
             AND q.snapshot_at = latest.snapshot_at
            """
        )
        if rows and int(rows[0].get("total") or 0) >= 3000:
            rows[0]["data_source"] = "sm_rt_quote_snapshot"
            return rows[0]
    except Exception as exc:
        _record_fallback('_get_realtime_overview:archive', exc)
    return None


def _sql_in_params(values: list[str], prefix: str) -> tuple[str, dict[str, str]]:
    params: dict[str, str] = {}
    placeholders: list[str] = []
    for idx, value in enumerate(values):
        key = f"{prefix}{idx}"
        placeholders.append(f":{key}")
        params[key] = value
    return ",".join(placeholders), params


def _monitor_resolve_trade_date(requested_date: str | None = None) -> str:
    requested = str(requested_date or "").strip() or date.today().isoformat()
    overview_date = ""
    if "trade_date" in _table_columns("sm_market_overview_daily"):
        rows = _read_sql(
            """
            SELECT trade_date AS d
            FROM sm_market_overview_daily
            WHERE total >= 3000 AND trade_date <= :d
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            {"d": requested[:10]},
        )
        if rows and rows[0].get("d"):
            overview_date = str(rows[0]["d"])[:10]
    kline_rows = _read_sql(
        """
        SELECT trade_date AS d
        FROM (
            SELECT trade_date, COUNT(*) AS cnt
            FROM sm_stock_kline
            WHERE k_type = 1 AND adjust_type = 0
              AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 45 DAY) AND :d
            GROUP BY trade_date
        ) t
        WHERE cnt >= 3000
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        {"d": requested[:10]},
    )
    kline_date = str(kline_rows[0]["d"])[:10] if kline_rows and kline_rows[0].get("d") else ""
    resolved = max(overview_date, kline_date)
    if resolved:
        return resolved
    latest_rows = _read_sql(
        """
        SELECT trade_date AS d
        FROM sm_stock_kline
        WHERE k_type = 1
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    if latest_rows and latest_rows[0].get("d"):
        return str(latest_rows[0]["d"])[:10]
    return requested[:10]


def _monitor_history_trade_dates(trade_date: str, limit: int = 20) -> list[str]:
    limit = max(1, min(int(limit), 60))
    kline_rows = _read_sql(f"""
        SELECT trade_date
        FROM (
            SELECT trade_date, COUNT(*) AS cnt
            FROM sm_stock_kline
            WHERE k_type = 1 AND adjust_type = 0
              AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 120 DAY) AND :d
            GROUP BY trade_date
            HAVING cnt >= 3000
            ORDER BY trade_date DESC
            LIMIT {limit}
        ) t
        ORDER BY trade_date
    """, {"d": trade_date[:10]})
    kline_result = [str(row.get("trade_date") or "")[:10] for row in kline_rows if row.get("trade_date")]
    if "trade_date" in _table_columns("sm_market_overview_daily"):
        rows = _read_sql(f"""
            SELECT trade_date
            FROM (
                SELECT trade_date
                FROM sm_market_overview_daily
                WHERE total >= 3000 AND trade_date <= :d
                ORDER BY trade_date DESC
                LIMIT {limit}
            ) t
            ORDER BY trade_date
        """, {"d": trade_date[:10]})
        result = [str(row.get("trade_date") or "")[:10] for row in rows if row.get("trade_date")]
        if result:
            if kline_result and kline_result[-1] > result[-1]:
                return kline_result
            return result
    return kline_result


def _monitor_overview_map_from_kline(trade_dates: list[str]) -> dict[str, dict]:
    if not trade_dates:
        return {}
    placeholders, params = _sql_in_params(trade_dates, "td")
    rows = _read_sql(f"""
        SELECT trade_date,
               SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
               SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt,
               SUM(CASE WHEN ABS(change_pct) < 1 THEN 1 ELSE 0 END) AS sideline_cnt,
               COUNT(*) AS total,
               COALESCE(SUM(amount), 0) AS total_amount,
               SUM(CASE
                     WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                      AND change_pct > 0 THEN 1 ELSE 0
                   END) AS small_up_cnt,
               SUM(CASE
                     WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                     THEN 1 ELSE 0
                   END) AS small_total,
               AVG(CASE
                     WHEN (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
                     THEN change_pct
                   END) AS small_avg_chg
        FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date IN ({placeholders})
        GROUP BY trade_date
        ORDER BY trade_date
    """, params)
    return {str(row["trade_date"])[:10]: row for row in rows if row.get("trade_date")}


def _monitor_overview_map(trade_dates: list[str]) -> dict[str, dict]:
    if not trade_dates:
        return {}
    if "trade_date" in _table_columns("sm_market_overview_daily"):
        placeholders, params = _sql_in_params(trade_dates, "mo")
        rows = _read_sql(f"""
            SELECT trade_date,
                   up_cnt,
                   down_cnt,
                   sideline_cnt,
                   total,
                   total_amount,
                   small_up_cnt,
                   small_total,
                   small_avg_chg
            FROM sm_market_overview_daily
            WHERE trade_date IN ({placeholders})
        """, params)
        result = {str(row["trade_date"])[:10]: row for row in rows if row.get("trade_date")}
        missing = [d for d in trade_dates if d not in result]
        if missing:
            result.update(_monitor_overview_map_from_kline(missing))
        return result
    return _monitor_overview_map_from_kline(trade_dates)


def _monitor_qmt_plate_rows(trade_date: str, *, use_current: bool = False) -> list[dict]:
    if not trade_date:
        return []
    live_source = trade_date[:10] == date.today().isoformat() and (use_current or _is_monitor_trading_time())
    try:
        if live_source:
            kline_rows = _read_sql("""
                SELECT stock_code, :trade_date AS trade_date, amount, change_pct
                FROM sm_stock_current
            """, {"trade_date": trade_date[:10]})
            if len(kline_rows) < 3000:
                return []
        else:
            kline_rows = _read_sql("""
                SELECT stock_code, trade_date, amount, change_pct
                FROM sm_stock_kline
                WHERE trade_date = :trade_date AND k_type = 1 AND adjust_type = 0
            """, {"trade_date": trade_date})
        plate_rows = _read_sql("""
            SELECT stock_code, plate_type, plate_name
            FROM si_stock_plate_east
            WHERE source IN ('qmt', 'gj_big_qmt_inner')
              AND plate_type IN (:concept_type, :industry_type)
        """, {"concept_type": "\u6982\u5ff5", "industry_type": "\u884c\u4e1a"})
    except Exception:
        return []

    if not kline_rows or not plate_rows:
        return []

    kline_df = pd.DataFrame(kline_rows)
    plate_df = pd.DataFrame(plate_rows)
    required_kline = {"stock_code", "trade_date", "amount", "change_pct"}
    required_plate = {"stock_code", "plate_type", "plate_name"}
    if not required_kline.issubset(kline_df.columns) or not required_plate.issubset(plate_df.columns):
        return []

    qmt_type_map = {"\u6982\u5ff5": 1, "\u884c\u4e1a": 3}
    plate_df = plate_df[plate_df["plate_type"].isin(qmt_type_map.keys())].copy()
    if plate_df.empty:
        return []

    kline_df["stock_code"] = kline_df["stock_code"].astype(str)
    plate_df["stock_code"] = plate_df["stock_code"].astype(str)
    merged = kline_df.merge(plate_df, on="stock_code", how="inner")
    if merged.empty:
        return []

    merged["amount"] = pd.to_numeric(merged["amount"], errors="coerce").fillna(0.0)
    merged["change_pct"] = pd.to_numeric(merged["change_pct"], errors="coerce").fillna(0.0)
    merged["plate_name"] = merged["plate_name"].astype(str)
    merged = merged[merged["plate_name"].str.len() > 0].copy()
    if merged.empty:
        return []

    merged["weighted_change"] = merged["amount"] * merged["change_pct"]
    grouped_df = (
        merged.groupby(["trade_date", "plate_type", "plate_name"], dropna=False)
        .agg(
            hot_amount=("amount", "sum"),
            weighted_change=("weighted_change", "sum"),
            avg_change=("change_pct", "mean"),
            stock_count=("stock_code", "nunique"),
        )
        .reset_index()
    )
    grouped_df = grouped_df[grouped_df["stock_count"] >= 2].copy()
    if grouped_df.empty:
        return []

    grouped_df["hot_value"] = (grouped_df["hot_amount"] / 100000000).round(2)
    grouped_df["change_pct"] = grouped_df.apply(
        lambda row: round(
            row["weighted_change"] / row["hot_amount"]
            if float(row["hot_amount"] or 0) > 0
            else row["avg_change"],
            2,
        ),
        axis=1,
    )

    rows: list[dict] = []
    for _, row in grouped_df.iterrows():
        plate_type = qmt_type_map.get(str(row.get("plate_type") or ""))
        if not plate_type:
            continue
        rows.append({
            "snapshot_date": str(row.get("trade_date") or "")[:10],
            "plate_type": int(plate_type),
            "concept_name": str(row.get("plate_name") or ""),
            "hot_value": float(row.get("hot_value") or 0),
            "change_pct": float(row.get("change_pct") or 0),
            "stock_count": int(row.get("stock_count") or 0),
            "data_source": "qmt_current_plate_aggregate" if live_source else "qmt_plate_aggregate",
        })
    rows.sort(key=lambda item: (item["plate_type"], -float(item.get("hot_value") or 0), item.get("concept_name") or ""))
    return rows


def _monitor_hot_rows_map(
    trade_dates: list[str],
    *,
    use_current: bool = False,
) -> dict[tuple[str, int], list[dict]]:
    if not trade_dates:
        return {}
    placeholders, params = _sql_in_params(trade_dates, "hd")
    grouped: dict[tuple[str, int], list[dict]] = {}

    try:
        fallback_rows = _read_sql(f"""
            SELECT snapshot_date, plate_type, concept_name, hot_value, change_pct, 'hot_rank_fallback' AS data_source
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date IN ({placeholders}) AND plate_type IN (1, 2, 3)
            ORDER BY snapshot_date, plate_type, hot_value DESC
        """, params)
    except Exception:
        fallback_rows = []

    for row in fallback_rows:
        snapshot_date = str(row.get("snapshot_date") or "")[:10]
        plate_type = int(row.get("plate_type") or 0)
        grouped.setdefault((snapshot_date, plate_type), []).append(row)

    latest_date = trade_dates[-1]
    qmt_keys: set[tuple[str, int]] = set()
    for row in _monitor_qmt_plate_rows(latest_date, use_current=use_current):
        snapshot_date = str(row.get("snapshot_date") or "")[:10]
        plate_type = int(row.get("plate_type") or 0)
        if snapshot_date and plate_type:
            key = (snapshot_date, plate_type)
            if key not in qmt_keys:
                grouped[key] = []
                qmt_keys.add(key)
            grouped[key].append(row)
    for key, rows in list(grouped.items()):
        rows.sort(key=lambda item: float(item.get("hot_value") or 0), reverse=True)
    return grouped


def _monitor_pick_rows(
    hot_rows_map: dict[tuple[str, int], list[dict]],
    trade_dates: list[str],
    preferred_plate_types: tuple[int, ...],
    limit: int = 10,
) -> list[dict]:
    for trade_date in trade_dates:
        for plate_type in preferred_plate_types:
            rows = hot_rows_map.get((trade_date, plate_type)) or []
            if rows:
                return rows[:limit]
    return []


def _monitor_tmt_ratio(industry_rows: list[dict]) -> float:
    if not industry_rows:
        return 0.0
    tmt_children = {
        "电子", "计算机", "通信", "传媒",
        "电子化学品", "半导体", "消费电子", "其他电子", "光学光电子", "元件",
        "软件开发", "IT服务", "计算机设备",
        "通信服务", "通信设备",
        "广告营销", "影视院线", "数字媒体", "游戏", "出版", "电视广播",
    }
    tmt_hot = sum(float(row.get("hot_value") or 0) for row in industry_rows if row.get("concept_name") in tmt_children)
    total_hot = sum(float(row.get("hot_value") or 0) for row in industry_rows)
    if total_hot <= 0:
        return 0.0
    return round(tmt_hot / total_hot * 100, 2)


def _monitor_index_price_map(trade_dates: list[str]) -> dict[str, dict]:
    if not trade_dates:
        return {}
    placeholders, params = _sql_in_params(trade_dates, "idx")
    rows = _read_sql(f"""
        SELECT trade_date, close AS price, change_pct
        FROM sm_index_kline
        WHERE index_code = '000852' AND k_type = 1 AND trade_date IN ({placeholders})
        ORDER BY trade_date
    """, params)
    return {str(row["trade_date"])[:10]: row for row in rows if row.get("trade_date")}


@router.get("/sector/movement")
def sector_movement(group_by: str = Query(default="industry", regex="^(industry|concept|all)$")):
    """板块异动检测 + 龙头识别

    group_by: industry=按行业分组(申万一级), concept=按概念分组, all=同时展示两组
    """
    _ttl = _market_live_cache_ttl()
    _ckey = f"sector_movement_{group_by}"
    cached = _cache_get(_ckey, ttl_seconds=_ttl)
    if cached is not None:
        return cached
    try:
        from collections import defaultdict

        now_snap = _read_sql("SELECT MAX(snapshot_at) AS sa FROM sm_rt_quote_snapshot")
        if not now_snap or not now_snap[0].get("sa"):
            return {"sectors": [], "error": "无实时数据"}
        now_sa = now_snap[0]["sa"]

        prev_snap = _read_sql(
            "SELECT MAX(snapshot_at) AS sa FROM sm_rt_quote_snapshot "
            "WHERE snapshot_at <= :t - INTERVAL 2 MINUTE",
            {"t": now_sa},
        )
        has_prev = bool(prev_snap and prev_snap[0].get("sa"))
        prev_sa = prev_snap[0]["sa"] if has_prev else None

        stock_limit = 1200
        min_abs_change = 0.3
        min_amount = 30_000_000
        raw = _read_sql("""
            SELECT stock_code, short_name, change_pct AS now_pct, amount AS now_amt
            FROM sm_rt_quote_snapshot
            WHERE snapshot_at = :nsa
              AND (ABS(COALESCE(change_pct, 0)) >= :min_abs_change OR COALESCE(amount, 0) >= :min_amount)
            ORDER BY ABS(COALESCE(change_pct, 0)) DESC, COALESCE(amount, 0) DESC
            LIMIT :stock_limit
        """, {"nsa": now_sa, "min_abs_change": min_abs_change, "min_amount": min_amount, "stock_limit": stock_limit})

        prev_pct_map = {}
        if has_prev:
            prev_rows = _read_sql("""
                SELECT stock_code, change_pct AS prev_pct
                FROM sm_rt_quote_snapshot
                WHERE snapshot_at = :psa
            """, {"psa": prev_sa})
            prev_pct_map = {str(r["stock_code"]): r.get("prev_pct") for r in prev_rows}
            for row in raw:
                row["prev_pct"] = prev_pct_map.get(str(row["stock_code"]))
        else:
            for row in raw:
                row["prev_pct"] = None

        codes = [str(r["stock_code"]) for r in raw]
        if not codes:
            return {"sectors": []}

        ph, code_params = _sql_in_params(codes, "mv")
        mapping_source: list[str] = []

        def _load_qmt_plate_map(plate_type: str):
            result_map = defaultdict(set)
            try:
                params = dict(code_params)
                params["pt"] = plate_type
                plate_rows = _read_sql(f"""
                    SELECT stock_code, plate_name
                    FROM si_stock_plate_east
                    WHERE stock_code IN ({ph}) AND plate_type = :pt
                """, params)
                for sr in plate_rows:
                    name = str(sr.get("plate_name") or "").strip()
                    if name:
                        result_map[str(sr["stock_code"])].add(name)
            except Exception:
                return defaultdict(set)
            return result_map

        # 获取概念板块映射
        concept_map = defaultdict(set)
        if group_by in ("concept", "all"):
            concept_map = _load_qmt_plate_map("概念")
            if concept_map:
                mapping_source.append("qmt_concept")
        if group_by in ("concept", "all") and not concept_map:
            try:
                concept_rows = _read_sql(f"""
                    SELECT stock_code, name AS plate_name
                    FROM si_stock_concept_map
                    WHERE stock_code IN ({ph})
                """, code_params)
                for sr in concept_rows:
                    concept_map[str(sr["stock_code"])].add(str(sr["plate_name"]))
            except Exception as exc:
                _record_fallback('sector_movement:7179', exc)

        # 获取行业板块映射（申万一级行业）
        industry_map = defaultdict(set)
        if group_by in ("industry", "all"):
            industry_map = _load_qmt_plate_map("行业")
            if industry_map:
                mapping_source.append("qmt_industry")
        if group_by in ("industry", "all") and not industry_map:
            try:
                industry_rows = _read_sql(f"""
                    SELECT stock_code, industry_name
                    FROM si_industry_sw
                    WHERE stock_code IN ({ph}) AND industry_type = '申万一级'
                """, code_params)
                for sr in industry_rows:
                    name = str(sr["industry_name"] or "").strip()
                    if name:
                        industry_map[str(sr["stock_code"])].add(name)
            except Exception as exc:
                _record_fallback('sector_movement:7199', exc)

        code_data = {}
        for r in raw:
            sc = str(r["stock_code"])
            now_p = float(r["now_pct"] or 0)
            prev_p = float(r["prev_pct"] or 0) if r.get("prev_pct") is not None else now_p
            code_data[sc] = {
                "code": sc,
                "name": str(r["short_name"] or sc),
                "change_pct": now_p,
                "momentum": round(now_p - prev_p, 2),
                "amount": float(r["now_amt"] or 0),
            }

        def _build_sector_list(groups_map):
            """从分组映射构建板块列表"""
            groups = defaultdict(list)
            for sc, plates in groups_map.items():
                if sc not in code_data:
                    continue
                for pn in plates:
                    groups[pn].append(code_data[sc])

            sector_list = []
            for sector, stocks in groups.items():
                if len(stocks) < 2:  # 过滤成分股太少的板块
                    continue
                # 龙头识别：按 成交额 * abs(动量) 综合排序
                stocks.sort(key=lambda s: s["amount"] * max(abs(s["momentum"]), 0.1), reverse=True)
                avg_chg = round(sum(s["change_pct"] for s in stocks) / len(stocks), 2)
                up = sum(1 for s in stocks if s["change_pct"] > 0)
                dn = sum(1 for s in stocks if s["change_pct"] < 0)
                leader = stocks[0] if stocks else None
                sector_list.append({
                    "name": sector,
                    "avg_change": avg_chg,
                    "stock_count": len(stocks),
                    "up_count": up,
                    "down_count": dn,
                    "leader": leader,
                    "top_movers": stocks[:10],
                })

            sector_list.sort(key=lambda s: abs(s["avg_change"]), reverse=True)
            return sector_list

        result = {
            "snapshot_time": str(now_sa),
            "has_momentum": has_prev,
            "group_by": group_by,
            "mapping_source": mapping_source or ["fallback"],
        }

        if group_by == "all":
            result["industry_sectors"] = _build_sector_list(industry_map)
            result["concept_sectors"] = _build_sector_list(concept_map)
            result["sectors"] = result["industry_sectors"]  # 默认展示行业
        elif group_by == "industry":
            result["sectors"] = _build_sector_list(industry_map)
        else:
            result["sectors"] = _build_sector_list(concept_map)

        _cache_set(_ckey, result)
        return result
    except Exception as e:
        return {"sectors": [], "error": str(e)}


@router.get("/monitor/data")
def monitor_data(date: str = Query(default_factory=lambda: date.today().isoformat())):
    """市场监控中心数据接口（盘中使用实时快照数据）"""
    requested_date = str(date or "").strip() or datetime.now().strftime("%Y-%m-%d")
    _ttl = _market_live_cache_ttl()
    _ckey = f"monitor_data_{requested_date}"
    cached = _cache_get(_ckey, ttl_seconds=_ttl)
    if cached is not None:
        return cached
    try:
        now_dt = datetime.now()
        today_text = now_dt.strftime("%Y-%m-%d")
        requests_today = requested_date[:10] == today_text
        today_is_trading_day = requests_today and _portfolio_is_trading_day(now_dt.date())
        wants_realtime = _is_monitor_trading_time() and today_is_trading_day
        clock_minute = now_dt.hour * 60 + now_dt.minute
        is_lunch_pause = now_dt.weekday() < 5 and 11 * 60 + 35 < clock_minute < 12 * 60 + 55
        allow_session_snapshot = (
            today_is_trading_day
            and (is_lunch_pause or (now_dt.hour, now_dt.minute) >= (15, 0))
        )
        current_overview = (
            _get_realtime_overview(allow_close=allow_session_snapshot)
            if wants_realtime or allow_session_snapshot
            else None
        )
        resolved_trade_date = _monitor_resolve_trade_date(requested_date)
        daily_history_dates = _monitor_history_trade_dates(resolved_trade_date, limit=20)

        if current_overview:
            # Daily bars may not exist until after the close.  A valid live
            # full-market snapshot is today's point, not yesterday relabelled.
            trade_date = today_text
            daily_history_dates = [item for item in daily_history_dates if item < trade_date]
            history_trade_dates = (daily_history_dates + [trade_date])[-20:]
            overview_map = _monitor_overview_map(daily_history_dates)
            prev_date = daily_history_dates[-1] if daily_history_dates else None
            cur = current_overview
            is_realtime = wants_realtime
        else:
            trade_date = resolved_trade_date
            history_trade_dates = daily_history_dates
            if not history_trade_dates:
                return {"error": "无交易数据"}
            overview_map = _monitor_overview_map(history_trade_dates)
            cur = overview_map.get(trade_date)
            if not cur or int(cur.get("total") or 0) <= 0:
                return {"error": f"交易日 {trade_date} 无数据"}
            prev_date = history_trade_dates[-2] if len(history_trade_dates) >= 2 else None
            is_realtime = False

        up_cnt = int(cur["up_cnt"] or 0)
        down_cnt = int(cur["down_cnt"] or 0)
        sideline_cnt = int(cur["sideline_cnt"] or 0)
        total = int(cur["total"] or 1)
        flat_cnt = max(0, total - up_cnt - down_cnt)
        total_amount = float(cur["total_amount"] or 0)
        sideline_ratio = round(sideline_cnt / total * 100, 2)
        market_heat = round(up_cnt / total * 1000, 0)
        small_total = int(cur.get("small_total") or 0)
        csi1000_heat = round(int(cur.get("small_up_cnt") or 0) / small_total * 1000, 0) if small_total > 0 else 0
        csi1000_chg = round(float(cur.get("small_avg_chg") or 0), 2) if small_total > 0 else 0.0

        prev_heat = 0
        if prev_date:
            prev_overview = overview_map.get(prev_date)
            if prev_overview and prev_overview.get("total"):
                prev_up = int(prev_overview.get("up_cnt") or 0)
                prev_total = int(prev_overview.get("total") or 1)
                prev_heat = round(prev_up / prev_total * 1000, 0)

        heat_change = round(((market_heat - prev_heat) / prev_heat * 100) if prev_heat > 0 else 0, 2)

        hot_rows_map = _monitor_hot_rows_map(
            history_trade_dates,
            use_current=bool(current_overview),
        )
        current_hot_dates = [trade_date] + ([prev_date] if prev_date else [])
        industry_rows = _monitor_pick_rows(hot_rows_map, current_hot_dates, (3, 2), limit=10)
        concept_rows = _monitor_pick_rows(hot_rows_map, current_hot_dates, (1,), limit=10)
        tmt_ratio = _monitor_tmt_ratio(industry_rows)

        index_price_map = _monitor_index_price_map(history_trade_dates)
        csi1000_price = float((index_price_map.get(trade_date) or {}).get("price") or 0)
        if is_realtime:
            try:
                current_index_rows = _read_sql(
                    "SELECT price, change_pct, snapshot_at FROM sm_index_current WHERE index_code = '000852' LIMIT 1"
                )
                if current_index_rows and current_index_rows[0].get("price") is not None:
                    current_index = current_index_rows[0]
                    snapshot_date = str(current_index.get("snapshot_at") or "")[:10]
                    if snapshot_date == datetime.now().strftime("%Y-%m-%d"):
                        csi1000_price = float(current_index.get("price") or csi1000_price or 0)
            except Exception as exc:
                _record_fallback('monitor_data:7338', exc)

        history_dates = []
        history_heat = []
        history_amount = []
        history_sideline = []
        history_tmt = []
        history_csi1000_heat = []

        for history_date in history_trade_dates:
            history_row = cur if (is_realtime and history_date == trade_date) else (overview_map.get(history_date) or {})
            history_dates.append(history_date[-5:])
            day_total = int(history_row.get("total") or 0)
            if day_total > 0:
                h = round(int(history_row.get("up_cnt") or 0) / day_total * 1000, 0)
                history_heat.append(h)
                history_amount.append(round(float(history_row.get("total_amount") or 0) / 1e8, 0))
                history_sideline.append(round(int(history_row.get("sideline_cnt") or 0) / day_total * 100, 2))
                history_tmt.append(_monitor_tmt_ratio(_monitor_pick_rows(hot_rows_map, [history_date], (3, 2), limit=10)))
                small_day_total = int(history_row.get("small_total") or 0)
                history_csi1000_heat.append(
                    round(int(history_row.get("small_up_cnt") or 0) / small_day_total * 1000, 0)
                    if small_day_total > 0 else 0
                )
            else:
                history_heat.append(0)
                history_amount.append(0)
                history_sideline.append(0)
                history_tmt.append(0)
                history_csi1000_heat.append(0)

        def calc_percentile(current, historical):
            valid = [h for h in historical if h > 0]
            if not valid:
                return 50
            below = sum(1 for h in valid if h < current)
            return round(below / len(valid) * 100, 0)

        heat_percentile = calc_percentile(market_heat, history_heat)

        heat_dir = "下降" if heat_change < 0 else "上升"
        heat_status = "偏冷" if market_heat < 400 else "偏热" if market_heat > 600 else "中性"

        top_industries = []
        if industry_rows:
            for r in industry_rows[:10]:
                top_industries.append({
                    "name": r.get("concept_name", ""),
                    "heat": round(float(r.get("hot_value") or 0), 0),
                    "change": round(float(r.get("change_pct") or 0), 2),
                    "trade_date": str(r.get("snapshot_date") or trade_date)[:10],
                    "data_source": r.get("data_source") or "",
                })

        signal = "低位徘徊" if market_heat < 400 else "高位运行" if market_heat > 600 else "震荡整理"
        data_time = str(cur.get("data_time") or trade_date)[:19]
        data_source = str(cur.get("data_source") or "daily_close")
        freshness_status = (
            "realtime"
            if is_realtime
            else "paused"
            if current_overview and is_lunch_pause
            else "close"
            if trade_date == requested_date[:10]
            else "fallback"
        )

        _result = {
            "trade_date": trade_date,
            "requested_date": requested_date[:10],
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_time": data_time,
            "data_source": data_source,
            "freshness_status": freshness_status,
            "is_realtime": is_realtime,
            "total_count": total,
            "up_count": up_cnt,
            "down_count": down_cnt,
            "flat_count": flat_cnt,
            "sideline_count": sideline_cnt,
            "market_heat": market_heat,
            "heat_change": heat_change,
            "heat_percentile": heat_percentile,
            "heat_status": heat_status,
            "total_amount": total_amount,
            "amount_display": f"{total_amount / 1e8:.0f}亿" if total_amount > 0 else "-",
            "sideline_ratio": sideline_ratio,
            "tmt_ratio": tmt_ratio,
            "csi1000": {
                "price": csi1000_price,
                "change": csi1000_chg,
                "heat": csi1000_heat,
            },
            "top_industries": top_industries,
            "concept_rows": [
                {"name": r.get("concept_name", ""), "heat": round(float(r.get("hot_value") or 0), 0), "change": round(float(r.get("change_pct") or 0), 2)}
                for r in (concept_rows or [])[:10]
            ],
            "history": {
                "dates": history_dates,
                "heat": history_heat,
                "amount": history_amount,
                "sideline": history_sideline,
                "tmt_ratio": history_tmt,
                "csi1000_heat": history_csi1000_heat,
            },
            "analysis": {
                "market_temp": f"全A热度{market_heat:.0f}，较昨日{heat_dir}{abs(heat_change):.2f}%，位于P{heat_percentile:.0f}{'低位' if heat_percentile < 30 else '高位' if heat_percentile > 70 else '中位'}，市场情绪{heat_status}",
                "industry_focus": f"热门行业：{', '.join(r['name'] for r in top_industries[:5])}" if top_industries else "暂无行业数据",
                "style_judge": f"中证1000 {csi1000_price:.0f}点，小盘热度{csi1000_heat:.0f}，涨跌{csi1000_chg:+.2f}%",
                "capital_flow": f"小波动个股占比{sideline_ratio:.2f}%，TMT成交占比{tmt_ratio:.2f}%",
                "signal": f"全A{signal}，关注{'周期股补涨机会' if market_heat < 400 else '科技板块轮动'}",
            },
        }
        _cache_set(_ckey, _result)
        return _result
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@router.get("/hot-data/command-monitor")
def command_monitor_data(date: str = Query(default_factory=lambda: date.today().isoformat())):
    """Fallback market monitor payload for the command dashboard."""
    return monitor_data(date)


@router.post("/strategy/picks/sync")
def strategy_picks_sync(body: dict):
    """接收聚宽策略选股结果（从聚宽调用）"""
    try:
        strategy_name = body.get("strategy_name")
        picks = body.get("picks", [])
        pick_date = body.get("pick_date", datetime.now().strftime("%Y-%m-%d"))
        description = body.get("description", "")

        if not strategy_name or not picks:
            return {"error": "strategy_name 和 picks 不能为空"}

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(
                text("DELETE FROM jq_strategy_picks WHERE strategy_name = :s AND pick_date = :d"),
                {"s": strategy_name, "d": pick_date},
            )

            for p in picks:
                code = str(p.get("stock_code", "")).strip().zfill(6)
                if not code or len(code) != 6:
                    continue
                conn.execute(
                    text("""
                        INSERT INTO jq_strategy_picks (
                            strategy_name, stock_code, short_name, score, reason,
                            pick_date, created_at
                        )
                        VALUES (:s, :c, :n, :sc, :r, :d, NOW())
                    """),
                    {
                        "s": strategy_name,
                        "c": code,
                        "n": p.get("short_name", ""),
                        "sc": p.get("score"),
                        "r": p.get("reason", ""),
                        "d": pick_date,
                    },
                )

            conn.execute(
                text("""
                    INSERT INTO jq_strategy_meta (
                        strategy_name, description, last_run_date,
                        created_at, updated_at
                    )
                    VALUES (:s, :desc, :d, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        last_run_date = :d,
                        description = IF(:desc != '', :desc, description),
                        updated_at = NOW()
                """),
                {"s": strategy_name, "desc": description, "d": pick_date},
            )
            conn.commit()

        return {"success": True, "count": len(picks), "strategy": strategy_name, "date": pick_date}
    except Exception as e:
        return {"error": str(e)}


@router.get("/strategy/picks/list")
def strategy_picks_list():
    """获取所有策略列表"""
    try:
        rows = _read_sql("""
            SELECT sm.*, 
                   (SELECT COUNT(*) FROM jq_strategy_picks jp WHERE jp.strategy_name = sm.strategy_name AND jp.pick_date = sm.last_run_date) AS last_count
            FROM jq_strategy_meta sm
            ORDER BY sm.updated_at DESC
        """)
        return {"strategies": rows}
    except Exception as e:
        return {"strategies": [], "error": str(e)}


@router.get("/strategy/picks/data")
def strategy_picks_data(
    strategy_name: str = Query(default=None),
    date: str = Query(default=None),
):
    """获取策略选股结果"""
    try:
        if not strategy_name:
            rows = _read_sql("SELECT DISTINCT strategy_name FROM jq_strategy_picks ORDER BY strategy_name")
            if not rows:
                return {"picks": [], "error": "暂无策略数据"}
            strategy_name = str(rows[0]["strategy_name"])

        if not date:
            rows = _read_sql(
                "SELECT MAX(pick_date) AS d FROM jq_strategy_picks WHERE strategy_name = :s",
                {"s": strategy_name},
            )
            date = str(rows[0]["d"])[:10] if rows and rows[0].get("d") else datetime.now().strftime("%Y-%m-%d")

        picks = _read_sql(
            "SELECT stock_code, short_name, score, reason, pick_date FROM jq_strategy_picks WHERE strategy_name = :s AND pick_date = :d ORDER BY score DESC",
            {"s": strategy_name, "d": date},
        )

        codes = [str(p["stock_code"]).zfill(6) for p in picks]
        rt_map = _portfolio_fetch_live_quotes(codes)

        result = []
        for p in picks:
            code = str(p["stock_code"]).zfill(6)
            name = str(p.get("short_name") or code)
            rt = rt_map.get(code, {})
            result.append({
                "stock_code": code,
                "short_name": rt.get("short_name") or name,
                "score": p.get("score"),
                "reason": p.get("reason", ""),
                "price": rt.get("price"),
                "change_pct": rt.get("change_pct"),
                "change_amt": rt.get("change_amt"),
                "amount": rt.get("amount"),
                "turnover_rate": rt.get("turnover_rate"),
            })

        return {
            "strategy_name": strategy_name,
            "date": date,
            "picks": result,
        }
    except Exception as e:
        return {"picks": [], "error": str(e)}


@router.post("/monitor/sync-realtime")
def sync_realtime_data():
    """盘中同步实时行情数据到数据库"""
    if not _job_begin("market_refresh"):
        return {"success": False, "busy": True, "error": "market_refresh_running"}
    try:
        from integrations.bigqmt.spool import PROVIDER_ID, resolve_big_qmt_home

        # The production API can run on Linux while the standard QMT terminal
        # and consumer run on the operator's Windows host.  In that topology
        # the database is already refreshed continuously; a web request must
        # never wait for or attempt to launch a desktop terminal remotely.
        if resolve_big_qmt_home(required=False) is None:
            latest = _read_sql(
                "SELECT COUNT(*) AS rows_count, MAX(snapshot_at) AS snapshot_at "
                "FROM sm_stock_current WHERE data_source = :source",
                {"source": PROVIDER_ID},
            )
            row = latest[0] if latest else {}
            return {
                "success": True,
                "source": PROVIDER_ID,
                "status": "managed_by_external_big_qmt_bridge",
                "synced": 0,
                "available_rows": int(row.get("rows_count") or 0),
                "snapshot_at": row.get("snapshot_at"),
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        from tools.run_big_qmt_bridge import sync_big_qmt_realtime

        # Current quotes are owned by the local QMT/public quote collector.
        # Even an explicit refresh must use the dedicated current-data route;
        # never write a live snapshot into production's primary database.
        engine = get_current_engine()
        result = sync_big_qmt_realtime(engine=engine)
        total_synced = int(result.get("full_rows") or 0) + int(result.get("tracked_rows") or 0)
        _invalidate_market_runtime_caches()
        return {
            "success": True,
            "source": result.get("source"),
            "status": result.get("status"),
            "synced": total_synced,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}
    finally:
        _job_end("market_refresh")


# ═══════════════════════════════════════════
# AI 推荐买入股票
# ═══════════════════════════════════════════

def _recommendation_theme_coverage(rows: list[dict], trade_date: str) -> dict:
    """Attach the full catalyst pool to the stock-level recommendation result.

    Theme coverage and stock admission are intentionally separate: a valid
    theme remains visible even when no stock passes the trading gate.
    """
    try:
        from biz.research_radar.radar import build_research_radar

        radar = build_research_radar(get_engine(), trade_date or None)
    except Exception as exc:
        return {
            "theme_overview": [],
            "theme_coverage": {
                "scanned_theme_count": 0,
                "active_theme_count": 0,
                "represented_theme_count": 0,
                "unrepresented_active_theme_count": 0,
                "error": str(exc),
            },
            "unclassified_catalysts": [],
            "theme_scan_dimensions": [],
            "theme_radar_version": "",
        }

    def _norm(value) -> str:
        return re.sub(r"\s+", "", str(value or "")).lower()

    theme_overview = []
    represented_theme_ids: set[str] = set()
    active_theme_ids: set[str] = set()
    for row in rows:
        row["research_themes"] = []

    for theme in radar.get("themes", []):
        theme_id = str(theme.get("id") or "")
        stock_codes = {
            str(stock.get("code") or "").zfill(6)
            for stock in theme.get("stocks", [])
            if stock.get("code")
        }
        keywords = [_norm(keyword) for keyword in theme.get("keywords", []) if keyword]
        matched_rows = []
        for row in rows:
            code = str(row.get("stock_code") or "").zfill(6)
            evidence_text = _norm(
                " ".join(
                    str(row.get(field) or "")
                    for field in (
                        "reason",
                        "recommend_reason",
                        "signal_reason",
                        "sector_gate_reason",
                        "sources",
                    )
                )
            )
            if code not in stock_codes and not any(keyword in evidence_text for keyword in keywords):
                continue
            matched_rows.append(row)
            row["research_themes"].append(
                {
                    "id": theme_id,
                    "name": theme.get("name"),
                    "score": theme.get("score"),
                    "rank_tier": theme.get("rank_tier"),
                    "status": theme.get("status"),
                }
            )

        if matched_rows:
            represented_theme_ids.add(theme_id)
        if theme.get("active"):
            active_theme_ids.add(theme_id)
        if theme.get("status") == "逻辑转弱":
            coverage_status = "逻辑转弱，仅作风险观察"
        elif matched_rows:
            coverage_status = f"已有 {len(matched_rows)} 只推荐候选"
        elif theme.get("active"):
            coverage_status = "主题成立，暂无股票通过门禁"
        else:
            coverage_status = "常规观察，等待新催化"

        theme_overview.append(
            {
                "rank": theme.get("rank"),
                "id": theme_id,
                "name": theme.get("name"),
                "category": theme.get("category"),
                "score": theme.get("score"),
                "rank_tier": theme.get("rank_tier"),
                "status": theme.get("status"),
                "active": bool(theme.get("active")),
                "trend": theme.get("trend"),
                "logic": theme.get("logic"),
                "verification": theme.get("verification"),
                "risk": theme.get("risk"),
                "trigger_labels": theme.get("trigger_labels") or [],
                "catalysts": [
                    {
                        "title": item.get("title"),
                        "source": item.get("source"),
                        "publish_time": item.get("publish_time"),
                        "direction": item.get("direction"),
                    }
                    for item in (theme.get("news_hits") or [])[:5]
                ],
                "market_hits": (theme.get("market_hits") or [])[:4],
                "candidate_count": len(matched_rows),
                "candidate_codes": [
                    str(row.get("stock_code") or "").zfill(6)
                    for row in matched_rows
                ],
                "candidate_names": [
                    str(row.get("short_name") or row.get("stock_code") or "")
                    for row in matched_rows
                ],
                "coverage_status": coverage_status,
            }
        )

    summary = dict(radar.get("coverage_summary") or {})
    summary.update(
        {
            "represented_theme_count": len(represented_theme_ids),
            "unrepresented_active_theme_count": len(active_theme_ids - represented_theme_ids),
            "radar_trade_date": radar.get("trade_date"),
            "radar_cutoff_at": radar.get("cutoff_at"),
        }
    )
    return {
        "theme_overview": theme_overview,
        "theme_coverage": summary,
        "unclassified_catalysts": radar.get("unclassified_catalysts") or [],
        "theme_scan_dimensions": radar.get("scan_dimensions") or [],
        "theme_radar_version": radar.get("version") or "",
    }


def _recommended_stocks_v2(
    trade_date: str,
    strategy: str = "",
    signal_status: str = "",
    start_date: str = "",
    end_date: str = "",
    allow_previous_snapshot: bool = False,
):
    columns = _table_columns("st_recommended_stocks")
    strategy = (strategy or "").strip()
    signal_status = (signal_status or "").strip().upper()

    select_cols = [
        _select_col(columns, "stock_code", "''"),
        _select_col(columns, "short_name", "''"),
        _select_col(columns, "ai_score", "0"),
        _select_col(columns, "fundamental", "0"),
        _select_col(columns, "capital_score", "0"),
        _select_col(columns, "valuation", "0"),
        _select_col(columns, "technical", "0"),
        _select_col(columns, "reason", "''"),
        _select_col(columns, "sources", "''"),
        _select_col(columns, "pick_date", "NULL"),
        _select_col(columns, "long_term_score", "0"),
        _select_col(columns, "short_term_score", "0"),
        _select_col(columns, "recommend_status", "'BLOCK'"),
        _select_col(columns, "recommend_reason", "''"),
        _select_col(columns, "chase_risk_status", "'DATA_BLOCKED'"),
        _select_col(columns, "ordinary_buy_eligible", "0"),
        _select_col(columns, "event_risk_level", "'LOW'"),
        _select_col(columns, "sentiment_score", "0"),
        _select_col(columns, "market_mood_score", "0"),
        _select_col(columns, "event_score", "0"),
        _select_col(columns, "ultra_short_score", "0"),
        _select_col(columns, "swing_score", "0"),
        _select_col(columns, "primary_strategy", "''"),
        _select_col(columns, "strategy_profile", "''"),
        _select_col(columns, "suitable_strategies", "'[]'"),
        _select_col(columns, "signal_status", "'WATCH'"),
        _select_col(columns, "signal_reason", "''"),
        _select_col(columns, "investment_rating", "'中性'"),
        _select_col(columns, "rating_reason", "''"),
        _select_col(columns, "entry_price_low", "NULL"),
        _select_col(columns, "entry_price_high", "NULL"),
        _select_col(columns, "stop_loss_price", "NULL"),
        _select_col(columns, "take_profit_1", "NULL"),
        _select_col(columns, "take_profit_2", "NULL"),
        _select_col(columns, "position_weight", "NULL"),
        _select_col(columns, "max_holding_days", "NULL"),
        _select_col(columns, "entry_conditions_json", "'[]'"),
        _select_col(columns, "sell_rules_json", "'[]'"),
        _select_col(columns, "invalidation_reason", "''"),
        _select_col(columns, "quality_score", "0"),
        _select_col(columns, "entry_score", "0"),
        _select_col(columns, "final_trade_score", "0"),
        _select_col(columns, "expected_return_score", "0"),
        _select_col(columns, "expected_return_pct", "0"),
        _select_col(columns, "risk_reward_ratio", "0"),
        _select_col(columns, "resistance_price", "NULL"),
        _select_col(columns, "sector_gate_status", "'WATCH'"),
        _select_col(columns, "sector_gate_reason", "''"),
        _select_col(columns, "sector_flow_3d", "NULL"),
        _select_col(columns, "sector_width_pct", "NULL"),
        _select_col(columns, "technical_evidence_json", "'{}'"),
        _select_col(columns, "evidence_chain_json", "'[]'"),
        _select_col(columns, "review_1d_pct", "NULL"),
        _select_col(columns, "review_3d_pct", "NULL"),
        _select_col(columns, "review_5d_pct", "NULL"),
        _select_col(columns, "review_10d_pct", "NULL"),
        _select_col(columns, "failure_tags_json", "'[]'"),
        _select_col(columns, "heat_overload_score", "0"),
        _select_col(columns, "confidence_score", "0"),
        _select_col(columns, "chip_capital_score", "0"),
        _select_col(columns, "sector_rotation_score", "0"),
        _select_col(columns, "failure_penalty_score", "0"),
        _select_col(columns, "cooldown_days_left", "0"),
        _select_col(columns, "cooldown_until", "NULL"),
        _select_col(columns, "main_wave_score", "0"),
        _select_col(columns, "trend_hold_score", "0"),
        _select_col(columns, "main_wave_stage", "''"),
        _select_col(columns, "main_wave_signal", "''"),
        _select_col(columns, "main_wave_reason", "''"),
        _select_col(columns, "trend_stop_price", "NULL"),
        _select_col(columns, "trend_reduce_price", "NULL"),
        _select_col(columns, "model_version", "''"),
        _select_col(columns, "price", "NULL"),
        _select_col(columns, "change_pct", "NULL"),
        _select_col(columns, "amount", "NULL"),
    ]

    def _order_sql() -> str:
        if "final_trade_score" in columns:
            strategy_score = {
                "ultra_short": "r.ultra_short_score",
                "short_term": "r.short_term_score",
                "swing": "r.swing_score",
                "main_wave": "r.main_wave_score",
            }.get(strategy, "r.final_trade_score")
            return f"{strategy_score} DESC, r.final_trade_score DESC, r.entry_score DESC"
        if strategy == "ultra_short" and "ultra_short_score" in columns:
            return "r.ultra_short_score DESC, r.ai_score DESC"
        if strategy == "swing" and "swing_score" in columns:
            return "r.swing_score DESC, r.ai_score DESC"
        if strategy == "short_term" and "short_term_score" in columns:
            return "r.short_term_score DESC, r.ai_score DESC"
        return "r.ai_score DESC"

    def _add_filters(conditions: list[str], params: dict) -> None:
        if strategy:
            if "suitable_strategies" in columns:
                conditions.append("r.suitable_strategies LIKE :strategy_like")
                params["strategy_like"] = f"%{strategy}%"
            elif strategy == "ultra_short" and "ultra_short_score" in columns:
                conditions.append("r.ultra_short_score >= 68")
            elif strategy == "swing" and "swing_score" in columns:
                conditions.append("r.swing_score >= 66")
            else:
                conditions.append("r.short_term_score >= 68")
        if signal_status and "signal_status" in columns:
            conditions.append("r.signal_status = :signal_status")
            params["signal_status"] = signal_status

    def _query_for_date(d: str) -> list[dict]:
        conditions = ["r.pick_date = :d"]
        params = {"d": d}
        _add_filters(conditions, params)

        return _read_sql(f"""
            SELECT {", ".join(select_cols)}
            FROM st_recommended_stocks r
            WHERE {" AND ".join(conditions)}
            ORDER BY {_order_sql()}
        """, params)

    def _query_for_range(start: str, end: str) -> list[dict]:
        conditions = ["r.pick_date >= :start_date", "r.pick_date <= :end_date"]
        params = {"start_date": start, "end_date": end}
        _add_filters(conditions, params)
        return _read_sql(f"""
            SELECT {", ".join(select_cols)}
            FROM st_recommended_stocks r
            WHERE {" AND ".join(conditions)}
            ORDER BY r.pick_date DESC, {_order_sql()}
            LIMIT 1000
        """, params)

    start_date = str(start_date or "").strip()[:10]
    end_date = str(end_date or "").strip()[:10]
    start_date = start_date if isinstance(start_date, str) else ""
    end_date = end_date if isinstance(end_date, str) else ""
    requested_trade_date = str(trade_date or "").strip()[:10]
    if start_date or end_date:
        if not end_date:
            end_date = start_date
        if not start_date:
            start_date = end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        rows = _query_for_range(start_date, end_date)
        trade_date = end_date
    else:
        if requested_trade_date:
            trade_date = requested_trade_date
        else:
            trade_date = _latest_date("st_recommended_stocks", "pick_date")
        rows = _query_for_date(trade_date) if trade_date else []
        if not rows and requested_trade_date and allow_previous_snapshot:
            fallback = _latest_date_not_after("st_recommended_stocks", requested_trade_date, "pick_date")
            if fallback and fallback != trade_date:
                trade_date = fallback
                rows = _query_for_date(trade_date)
        if not rows and not requested_trade_date:
            fallback = _latest_date("st_recommended_stocks", "pick_date")
            if fallback and fallback != trade_date:
                trade_date = fallback
                rows = _query_for_date(trade_date)
    if not rows:
        payload = {
            "date": trade_date,
            "data": [],
            "total": 0,
            "note": "no recommendation data",
            "freshness": _recommended_data_freshness(
                requested_date=requested_trade_date,
                result_date=trade_date,
                start_date=start_date,
                end_date=end_date,
                total=0,
            ),
        }
        payload.update(
            _recommendation_theme_coverage(
                [],
                requested_trade_date or trade_date or date.today().isoformat(),
            )
        )
        if start_date or end_date:
            payload.update({"start_date": start_date, "end_date": end_date, "note": "no recommendation data in date range"})
        return payload

    empty_name_codes = [r["stock_code"] for r in rows if not r.get("short_name")]
    if empty_name_codes:
        ph = ",".join(f"'{c}'" for c in empty_name_codes)
        name_rows = _read_sql(f"SELECT stock_code, short_name FROM si_all_code WHERE stock_code IN ({ph})")
        name_map = {nr["stock_code"]: nr["short_name"] for nr in name_rows if nr.get("short_name")}
        for r in rows:
            if not r.get("short_name"):
                r["short_name"] = name_map.get(r["stock_code"]) or r["stock_code"]

    current_quotes = {}
    live_quote_count = 0
    if _is_monitor_trading_time():
        try:
            current_quotes = _portfolio_fetch_live_quotes([r["stock_code"] for r in rows])
        except Exception:
            current_quotes = {}
        for r in rows:
            q = current_quotes.get(r["stock_code"], {})
            if q:
                live_quote_count += 1
                r["price"] = q.get("price")
                r["change_pct"] = q.get("change_pct")
                r["amount"] = q.get("amount")
                if q.get("short_name") and not r.get("short_name"):
                    r["short_name"] = q.get("short_name")

    quote_codes = [str(r["stock_code"]).zfill(6) for r in rows if r.get("stock_code")]
    if quote_codes:
        placeholders = ",".join(f"'{c}'" for c in quote_codes)
        quotes: dict[str, dict] = {}
        try:
            snapshot_date = _latest_date_not_after("sm_stock_snapshot", trade_date) or _latest_date("sm_stock_snapshot")
            if snapshot_date:
                q_rows = _read_sql(f"""
                    SELECT stock_code, price, change_pct, amount, trade_date
                    FROM sm_stock_snapshot
                    WHERE trade_date = :d
                      AND stock_code IN ({placeholders})
                """, {"d": snapshot_date})
                for q in q_rows:
                    q["quote_trade_date"] = str(q.get("trade_date") or snapshot_date)[:10]
                    q["quote_source"] = "snapshot"
                    quotes[str(q["stock_code"]).zfill(6)] = q
        except Exception:
            quotes = {}
        try:
            kline_date = _latest_date_not_after("sm_stock_kline", trade_date) or _latest_date("sm_stock_kline")
            if kline_date:
                k_rows = _read_sql(f"""
                    SELECT stock_code, close AS price, change_pct, amount, trade_date
                    FROM sm_stock_kline
                    WHERE k_type = 1
                      AND trade_date = :d
                      AND stock_code IN ({placeholders})
                """, {"d": kline_date})
                for q in k_rows:
                    code = str(q["stock_code"]).zfill(6)
                    q["quote_trade_date"] = str(q.get("trade_date") or kline_date)[:10]
                    q["quote_source"] = "daily_kline"
                    old = quotes.get(code)
                    if not old or str(q.get("quote_trade_date") or "") >= str(old.get("quote_trade_date") or ""):
                        quotes[code] = q
        except Exception as exc:
            _record_fallback('strategy_picks_quote_kline:7924', exc)
        for r in rows:
            code = str(r.get("stock_code") or "").zfill(6)
            if code in current_quotes:
                r["quote_source"] = "live"
                r["quote_trade_date"] = date.today().isoformat()
                continue
            q = quotes.get(code, {})
            if q:
                r["price"] = q.get("price")
                r["change_pct"] = q.get("change_pct")
                r["amount"] = q.get("amount")
                r["quote_trade_date"] = q.get("quote_trade_date")
                r["quote_source"] = q.get("quote_source")

    for r in rows:
        parsed = _parse_snapshot_json_fields(
            r,
            ("technical_evidence_json", "evidence_chain_json", "failure_tags_json", "entry_conditions_json", "sell_rules_json"),
        )
        if parsed:
            r.update(parsed)

    theme_payload = _recommendation_theme_coverage(
        rows,
        requested_trade_date or trade_date or date.today().isoformat(),
    )
    payload = {
        "date": trade_date,
        "data": rows,
        "total": len(rows),
        "strategy": strategy or "all",
        "signal_status": signal_status or "all",
        "model_version": rows[0].get("model_version") if rows else "",
        "freshness": _recommended_data_freshness(
            requested_date=requested_trade_date,
            result_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            live_quote_count=live_quote_count,
            total=len(rows),
        ),
    }
    payload.update(theme_payload)
    if start_date or end_date:
        payload.update({"start_date": start_date, "end_date": end_date})
    return payload


@router.get("/hot-data/recommended-stocks")
def recommended_stocks(
    trade_date: str = Query(default=""),
    strategy: str = Query(default=""),
    signal_status: str = Query(default=""),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    prefer_latest: bool = Query(default=False),
):
    # Direct unit-test calls bypass FastAPI parsing, so Query(default=False)
    # arrives as a Query object rather than a bool.
    prefer_latest = prefer_latest if isinstance(prefer_latest, bool) else False
    """获取 AI 推荐买入股票列表"""
    start_date = start_date if isinstance(start_date, str) else ""
    end_date = end_date if isinstance(end_date, str) else ""
    if start_date or end_date:
        cache_key = f"recommended_stocks_range_{start_date or 'open'}_{end_date or 'open'}_{strategy or 'all'}_{signal_status or 'all'}"
    else:
        cache_key = f"recommended_stocks_{trade_date or 'latest'}_{strategy or 'all'}_{signal_status or 'all'}{'_prefer_latest' if prefer_latest else ''}"
    cached = _cache_get(cache_key, ttl_seconds=_trading_live_ttl_seconds(300, intraday_seconds=30))
    if cached is not None:
        return cached
    try:
        if start_date or end_date:
            result = _recommended_stocks_v2(trade_date, strategy, signal_status, start_date, end_date)
        elif prefer_latest:
            result = _recommended_stocks_v2(trade_date, strategy, signal_status, allow_previous_snapshot=True)
        else:
            result = _recommended_stocks_v2(trade_date, strategy, signal_status)
        if isinstance(result, dict) and not result.get("error"):
            _cache_set(cache_key, result)
        return result
    except Exception as e:
        return {"date": trade_date, "data": [], "total": 0, "error": str(e)}


def _strategy_runtime_params_snapshot(as_of_date: str = "") -> dict:
    as_of_date = str(as_of_date or "").strip()[:10]
    if not as_of_date:
        try:
            as_of_date = _latest_date("st_recommended_stocks", "pick_date") or date.today().isoformat()
        except Exception:
            as_of_date = date.today().isoformat()
    try:
        from biz.analysis.sync_analysis_fast import DEFAULT_RUNTIME_PARAMS
    except Exception:
        DEFAULT_RUNTIME_PARAMS = {
            "min_risk_reward": 3.0,
            "min_sector_flow_amount_3d": 500_000_000.0,
            "min_sector_rotation_score": 50.0,
            "price_crosscheck_tolerance_pct": 1.0,
        }

    params = {
        key: {
            "param_key": key,
            "param_value": float(value),
            "source": "default",
            "effective_date": "",
            "updated_at": "",
            "metadata": {},
        }
        for key, value in DEFAULT_RUNTIME_PARAMS.items()
    }
    try:
        rows = _read_sql("""
            SELECT param_key, param_value, source, effective_date, updated_at, metadata_json
            FROM st_strategy_runtime_params
            WHERE status = 'active'
              AND (effective_date IS NULL OR effective_date <= :as_of_date)
            ORDER BY effective_date DESC, updated_at DESC
        """, {"as_of_date": as_of_date})
        for row in rows:
            key = str(row.get("param_key") or "")
            if key not in params:
                continue
            metadata = {}
            raw_metadata = row.get("metadata_json")
            if isinstance(raw_metadata, str) and raw_metadata:
                try:
                    metadata = json.loads(raw_metadata)
                except Exception:
                    metadata = {}
            params[key].update({
                "param_value": float(row.get("param_value") or params[key]["param_value"]),
                "source": row.get("source") or "runtime",
                "effective_date": str(row.get("effective_date") or "")[:10],
                "updated_at": str(row.get("updated_at") or ""),
                "metadata": metadata,
            })
    except Exception as exc:
        _record_fallback('_strategy_runtime_params_snapshot:7962', exc)

    calibration = {}
    try:
        cal_rows = _read_sql("""
            SELECT calibration_date, window_days, scope_type, scope_key,
                   sample_count, avg_return_5d, win_rate_5d, suggestion
            FROM st_strategy_threshold_calibration
            WHERE calibration_date <= :as_of_date
            ORDER BY calibration_date DESC, sample_count DESC
            LIMIT 1
        """, {"as_of_date": as_of_date})
        calibration = cal_rows[0] if cal_rows else {}
    except Exception:
        calibration = {}

    return {
        "as_of_date": as_of_date,
        "params": list(params.values()),
        "params_map": {key: item["param_value"] for key, item in params.items()},
        "calibration": calibration,
        "source": "st_strategy_runtime_params",
    }


@router.get("/hot-data/strategy-runtime-params")
def strategy_runtime_params(as_of_date: str = Query(default="")):
    """Return currently active stock strategy runtime thresholds."""
    try:
        return _strategy_runtime_params_snapshot(as_of_date)
    except Exception as e:
        return {"as_of_date": as_of_date, "params": [], "params_map": {}, "error": str(e)}


def _smart_trade_date() -> str:
    """智能判断当前应该使用的交易日期：
    - 盘中（工作日 9:30-15:00）：使用今天日期
    - 盘后/周末：使用最新已有数据的交易日
    """
    from datetime import datetime
    now = datetime.now()
    weekday = now.weekday()  # 0=周一, 6=周日
    hour_min = now.hour * 60 + now.minute
    # 盘中判断：工作日 9:30(570) - 15:00(900)
    is_trading = weekday < 5 and 570 <= hour_min <= 900
    if is_trading:
        return now.strftime("%Y-%m-%d")
    return _latest_date("sm_stock_kline")


def _parse_recommendation_execution_time(execution_time: str = "") -> datetime:
    raw = str(execution_time or "").strip()[:19]
    if not raw:
        return datetime.now().replace(microsecond=0)
    for candidate in (raw.replace("T", " "), raw):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError as exc:
            _record_fallback("_parse_recommendation_execution_time", exc)
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().replace(microsecond=0)


def _previous_recommendation_trade_date(engine, execution_time: str, today: str, expected_trade_date: str) -> str:
    try:
        from biz.analysis.sync_analysis_fast import previous_trade_date

        return previous_trade_date(engine, execution_time)
    except Exception as exc:
        _record_fallback("_previous_recommendation_trade_date", exc)
    latest_data_date = ""
    try:
        latest_data_date = _market_clock_latest_data_date(today)
    except Exception:
        latest_data_date = ""
    fallback = _market_clock_recommendation_trade_date(today, expected_trade_date, latest_data_date)
    return fallback or latest_data_date or expected_trade_date or today


def _resolve_recommended_run_context(
    *,
    trade_date: str = "",
    strict_prev_trade_day: bool = False,
    execution_time: str = "",
    refresh_realtime: bool = True,
    date_policy: str = "auto",
) -> dict:
    now_dt = _parse_recommendation_execution_time(execution_time)
    execution_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    today = now_dt.date().isoformat()
    requested_trade_date = str(trade_date or "").strip()[:10]
    policy = str(date_policy or "auto").strip().lower()
    if policy not in {"auto", "manual", "explicit", "requested", "request"}:
        policy = "auto"

    expected_trade_date = _market_clock_trade_date_from_calendar(today)
    is_trade_day = expected_trade_date == today
    phase, phase_label, _is_intraday = _market_phase(now_dt, is_trade_day)
    hhmm = now_dt.hour * 100 + now_dt.minute
    use_current_data_window = bool(is_trade_day and 925 <= hhmm <= 1505)
    engine = get_engine()

    if policy != "auto":
        if strict_prev_trade_day:
            resolved_trade_date = _previous_recommendation_trade_date(
                engine,
                execution_text,
                today,
                expected_trade_date,
            )
            resolved_strict = True
            resolved_refresh = False
            use_intraday_current = False
            source = "manual_previous_trade_day"
        else:
            resolved_trade_date = requested_trade_date or _latest_date("sm_stock_kline")
            resolved_strict = False
            use_intraday_current = bool(
                refresh_realtime
                and use_current_data_window
                and resolved_trade_date == today
            )
            resolved_refresh = bool(refresh_realtime and use_intraday_current)
            source = "manual_request"
    elif use_current_data_window:
        resolved_trade_date = today
        resolved_strict = False
        resolved_refresh = True
        use_intraday_current = True
        source = "intraday_current"
    else:
        resolved_trade_date = _previous_recommendation_trade_date(
            engine,
            execution_text,
            today,
            expected_trade_date,
        )
        resolved_strict = True
        resolved_refresh = False
        use_intraday_current = False
        source = "previous_trade_day"

    return {
        "date_policy": "manual" if policy != "auto" else "auto",
        "date_source": source,
        "requested_trade_date": requested_trade_date,
        "trade_date": str(resolved_trade_date or "")[:10],
        "strict_prev_trade_day": bool(resolved_strict),
        "requested_strict_prev_trade_day": bool(strict_prev_trade_day),
        "execution_time": execution_text,
        "refresh_realtime": bool(resolved_refresh),
        "use_intraday_current": bool(use_intraday_current),
        "market_phase": phase,
        "market_phase_label": phase_label,
        "is_trade_day": bool(is_trade_day),
        "expected_trade_date": expected_trade_date,
        "data_cutoff_time": execution_text if use_intraday_current else "",
    }


def _smart_trade_date() -> str:
    return _resolve_recommended_run_context(date_policy="auto")["trade_date"]


def _recommended_data_freshness(
    *,
    requested_date: str = "",
    result_date: str = "",
    start_date: str = "",
    end_date: str = "",
    live_quote_count: int = 0,
    total: int = 0,
) -> dict:
    requested_date = str(requested_date or "").strip()[:10]
    result_date = str(result_date or "").strip()[:10]
    start_date = str(start_date or "").strip()[:10]
    end_date = str(end_date or "").strip()[:10]
    reference_date = result_date or end_date or requested_date
    source_defs = [
        ("kline", "K线", "sm_stock_kline", "trade_date"),
        ("snapshot", "行情快照", "sm_stock_snapshot", "trade_date"),
        ("capital_flow", "资金流", "sm_stock_capital_flow_daily", "trade_date"),
        ("hot_rank", "热度榜", "st_hot_rank_fused", "snapshot_date"),
    ]
    sources = []
    stale_sources = []
    for key, label, table, col in source_defs:
        latest = ""
        try:
            latest = str(_latest_date(table, col) or "")[:10]
        except Exception:
            latest = ""
        stale = bool(reference_date and latest and latest < reference_date)
        if stale:
            stale_sources.append(label)
        sources.append({
            "key": key,
            "label": label,
            "table": table,
            "latest_date": latest,
            "stale": stale,
        })
    is_range = bool(start_date or end_date)
    is_fallback_date = bool(requested_date and result_date and result_date != requested_date and not is_range)
    is_missing_date = bool(
        requested_date
        and result_date == requested_date
        and not is_range
        and int(total or 0) == 0
    )
    status = "fresh"
    status_label = "数据日期匹配"
    if is_missing_date:
        status = "missing"
        status_label = "目标日推荐未生成"
    elif stale_sources:
        kline_source = next((s for s in sources if s.get("key") == "kline"), {})
        if stale_sources == ["行情快照"] and reference_date and str(kline_source.get("latest_date") or "") >= reference_date:
            status = "fresh"
            status_label = "日K已补行情"
        else:
            status = "stale"
            status_label = "基础数据滞后"
    elif is_fallback_date:
        status = "fallback"
        status_label = "已回退到最近推荐日"
    return {
        "status": status,
        "status_label": status_label,
        "requested_date": requested_date,
        "result_date": result_date,
        "start_date": start_date,
        "end_date": end_date,
        "is_range": is_range,
        "is_fallback_date": is_fallback_date,
        "is_missing_date": is_missing_date,
        "reference_date": reference_date,
        "sources": sources,
        "stale_sources": stale_sources,
        "live_quote_count": int(live_quote_count or 0),
        "total": int(total or 0),
        "quote_mode": "live" if live_quote_count else "stored",
        "is_intraday": bool(_is_monitor_trading_time()),
    }


def _recommendation_gate_status(
    execution_time: str = "",
    min_kline_coverage: float = 0.80,
    target_trade_date: str = "",
    check_readiness: bool = True,
) -> dict:
    """Return the strict premarket recommendation gate state.

    The morning job must use the previous trading day of the execution date.
    This helper reports that target date and whether local data is fresh enough
    to generate recommendations without silently falling back to older data.
    """
    from biz.analysis.sync_analysis_fast import assert_trade_date_ready, previous_trade_date

    engine = get_engine()
    execution_time = (execution_time or "").strip()
    if not execution_time:
        execution_time = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    min_kline_coverage = max(0.0, min(1.0, float(min_kline_coverage or 0.80)))

    target_trade_date = str(target_trade_date or "").strip()[:10]
    expected_trade_date = target_trade_date or previous_trade_date(engine, execution_time)
    payload = {
        "status": "ok",
        "execution_time": execution_time,
        "expected_trade_date": expected_trade_date,
        "target_source": "request" if target_trade_date else "previous_trade_date",
        "min_kline_coverage": min_kline_coverage,
        "ready": False,
        "ready_source": "",
        "error": "",
    }

    if check_readiness:
        try:
            payload["readiness"] = assert_trade_date_ready(
                engine,
                expected_trade_date,
                min_coverage=min_kline_coverage,
            )
            payload["ready"] = True
            payload["ready_source"] = "strict_readiness"
        except Exception as exc:
            payload["error"] = str(exc)
            payload["readiness"] = {
                "trade_date": expected_trade_date,
                "min_coverage": min_kline_coverage,
            }
    else:
        payload["readiness"] = {
            "trade_date": expected_trade_date,
            "min_coverage": min_kline_coverage,
            "skipped": True,
            "reason": "summary mode",
        }

    try:
        rows = _read_sql("""
            SELECT
                COUNT(*) AS rec_count,
                MAX(created_at) AS latest_created_at,
                SUM(CASE WHEN recommend_status = 'ALLOW'
                              AND chase_risk_status = 'ALLOW'
                              AND ordinary_buy_eligible = 1
                              AND COALESCE(signal_status, '') IN ('BUY_READY', 'CONFIRM')
                         THEN 1 ELSE 0 END) AS actionable_count
            FROM st_recommended_stocks
            WHERE pick_date = :d
        """, {"d": expected_trade_date})
        rec = rows[0] if rows else {}
        payload["recommendation"] = {
            "date": expected_trade_date,
            "count": int(rec.get("rec_count") or 0),
            "actionable_count": int(rec.get("actionable_count") or 0),
            "latest_created_at": str(rec.get("latest_created_at") or ""),
        }
    except Exception as exc:
        payload["recommendation"] = {"date": expected_trade_date, "count": 0, "error": str(exc)}

    try:
        analysis_rows = _read_sql("""
            SELECT COUNT(*) AS analysis_count, MAX(updated_at) AS latest_updated_at
            FROM stock_analysis_result
            WHERE analysis_date = :d
        """, {"d": expected_trade_date})
        ar = analysis_rows[0] if analysis_rows else {}
        payload["analysis"] = {
            "date": expected_trade_date,
            "count": int(ar.get("analysis_count") or 0),
            "latest_updated_at": str(ar.get("latest_updated_at") or ""),
        }
    except Exception as exc:
        payload["analysis"] = {"date": expected_trade_date, "count": 0, "error": str(exc)}

    try:
        news_rows = _read_sql("""
            SELECT COUNT(*) AS news_count, MAX(publish_time) AS latest_news_time
            FROM st_news_flash
            WHERE publish_time >= CONCAT(:d, ' 00:00:00')
              AND publish_time <= :execution_time
        """, {"d": expected_trade_date, "execution_time": execution_time[:19]})
        nr = news_rows[0] if news_rows else {}
        payload["news"] = {
            "from_date": expected_trade_date,
            "cutoff_time": execution_time[:19],
            "count": int(nr.get("news_count") or 0),
            "latest_news_time": str(nr.get("latest_news_time") or ""),
        }
    except Exception as exc:
        payload["news"] = {
            "from_date": expected_trade_date,
            "cutoff_time": execution_time[:19],
            "count": 0,
            "error": str(exc),
        }

    payload["has_recommendation"] = bool((payload.get("recommendation") or {}).get("count"))
    if not check_readiness and payload["has_recommendation"]:
        payload["ready"] = True
        payload["ready_source"] = "existing_recommendation"
    try:
        from tools.repair_recommendation_data import coverage_report

        payload["data_readiness"] = coverage_report(engine, expected_trade_date)
    except Exception as exc:
        payload["data_readiness"] = {
            "status": "unknown",
            "trade_date": expected_trade_date,
            "error": str(exc)[:500],
        }
    payload["strict_ok"] = bool(payload["ready"] and check_readiness)
    return payload


@router.get("/hot-data/recommended-stocks/gate")
def recommended_stocks_gate(
    execution_time: str = Query(default=""),
    min_kline_coverage: float = Query(default=0.80, ge=0, le=1),
    target_trade_date: str = Query(default=""),
    check_readiness: bool = Query(default=True),
):
    """Check the strict previous-trading-day gate for AI recommendations."""
    try:
        return _recommendation_gate_status(
            execution_time=execution_time,
            min_kline_coverage=min_kline_coverage,
            target_trade_date=target_trade_date,
            check_readiness=check_readiness,
        )
    except Exception as e:
        return {
            "status": "error",
            "ready": False,
            "strict_ok": False,
            "error": str(e),
        }


def _set_recommendation_progress(**payload) -> None:
    base = {
        "status": "idle",
        "percent": 0,
        "step": "",
        "total": 0,
        "done": 0,
        "passed": 0,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    base.update(payload)
    _cache_set("rec_screen_progress", base)


def _ensure_recommended_run_history_table() -> None:
    _exec_sql("""
        CREATE TABLE IF NOT EXISTS `st_recommended_run_history` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `run_uid` VARCHAR(40) NOT NULL,
            `trade_date` DATE NULL,
            `status` VARCHAR(20) NOT NULL DEFAULT 'running',
            `min_score` DECIMAL(8,2) NULL,
            `top_n` INT NULL,
            `strict_prev_trade_day` TINYINT(1) NOT NULL DEFAULT 0,
            `execution_time` DATETIME NULL,
            `started_at` DATETIME NULL,
            `finished_at` DATETIME NULL,
            `duration_seconds` INT NULL,
            `progress_percent` INT NULL,
            `done_count` INT NULL,
            `total` INT NULL,
            `passed` INT NULL,
            `flow_date` VARCHAR(20) NULL,
            `hot_date` VARCHAR(20) NULL,
            `market_mood_score` DECIMAL(8,2) NULL,
            `message` VARCHAR(500) NULL,
            `error` VARCHAR(500) NULL,
            `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_rec_run_uid` (`run_uid`),
            KEY `idx_rec_run_date` (`trade_date`, `started_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """, {})
    _recommended_history_ensure_column(
        "progress_percent",
        "ALTER TABLE `st_recommended_run_history` ADD COLUMN `progress_percent` INT NULL AFTER `duration_seconds`",
    )
    _recommended_history_ensure_column(
        "done_count",
        "ALTER TABLE `st_recommended_run_history` ADD COLUMN `done_count` INT NULL AFTER `progress_percent`",
    )


def _recommended_history_ensure_column(column: str, ddl: str) -> None:
    try:
        rows = _read_sql("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'st_recommended_run_history'
              AND column_name = :column
        """, {"column": column})
        if not rows or int(rows[0].get("cnt") or 0) == 0:
            _exec_sql(ddl)
            _cache_drop("table_columns_st_recommended_run_history")
    except Exception as exc:
        _record_fallback(f'_recommended_history_ensure_column:{column}', exc)


def _recommended_run_history_start(
    *,
    trade_date: str,
    min_score: float,
    top_n: int,
    strict_prev_trade_day: bool,
    execution_time: str,
    message: str = "",
    status: str = "running",
) -> str:
    import uuid

    run_uid = uuid.uuid4().hex
    try:
        _ensure_recommended_run_history_table()
        _exec_sql("""
            INSERT INTO st_recommended_run_history
            (run_uid, trade_date, status, min_score, top_n, strict_prev_trade_day,
             execution_time, started_at, progress_percent, done_count, message)
            VALUES (:uid, :trade_date, :status, :min_score, :top_n, :strict_prev,
                    :execution_time, NOW(), :progress_percent, 0, :message)
        """, {
            "uid": run_uid,
            "status": status[:20] if status else "running",
            "trade_date": trade_date,
            "min_score": float(min_score),
            "top_n": int(top_n),
            "strict_prev": 1 if strict_prev_trade_day else 0,
            "execution_time": execution_time[:19] if execution_time else None,
            "progress_percent": 5 if status == "queued" else 0,
            "message": message[:500] if message else "",
        })
    except Exception as exc:
        _record_fallback('_recommended_run_history_start:8297', exc)
    return run_uid


def _recommended_run_history_update(run_uid: str, *, status: str = "running", payload: dict | None = None) -> None:
    if not run_uid:
        return
    payload = payload or {}
    try:
        _ensure_recommended_run_history_table()
        _exec_sql("""
            UPDATE st_recommended_run_history
            SET status = COALESCE(:status, status),
                progress_percent = COALESCE(:progress_percent, progress_percent),
                done_count = COALESCE(:done_count, done_count),
                total = COALESCE(:total, total),
                passed = COALESCE(:passed, passed),
                flow_date = COALESCE(:flow_date, flow_date),
                hot_date = COALESCE(:hot_date, hot_date),
                market_mood_score = COALESCE(:market_mood_score, market_mood_score),
                message = COALESCE(:message, message),
                error = COALESCE(:error, error)
            WHERE run_uid = :uid
        """, {
            "uid": run_uid,
            "status": status[:20] if status else None,
            "progress_percent": payload.get("progress_percent"),
            "done_count": payload.get("done_count"),
            "total": payload.get("total"),
            "passed": payload.get("passed"),
            "flow_date": str(payload.get("flow_date") or "")[:20] if "flow_date" in payload else None,
            "hot_date": str(payload.get("hot_date") or "")[:20] if "hot_date" in payload else None,
            "market_mood_score": payload.get("market_mood_score"),
            "message": str(payload.get("message") or "")[:500] if "message" in payload else None,
            "error": str(payload.get("error") or "")[:500] if "error" in payload else None,
        })
    except Exception as exc:
        _record_fallback('_recommended_run_history_update', exc)


def _recommended_run_history_finish(run_uid: str, *, status: str, payload: dict | None = None) -> None:
    if not run_uid:
        return
    payload = payload or {}
    progress_percent = payload.get("progress_percent")
    if progress_percent is None and status == "done":
        progress_percent = 100
    done_count = payload.get("done_count")
    if done_count is None and status == "done":
        done_count = payload.get("total")
    try:
        _ensure_recommended_run_history_table()
        _exec_sql("""
            UPDATE st_recommended_run_history
            SET status = :status,
                trade_date = COALESCE(:trade_date, trade_date),
                finished_at = NOW(),
                duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                progress_percent = :progress_percent,
                done_count = :done_count,
                total = :total,
                passed = :passed,
                flow_date = :flow_date,
                hot_date = :hot_date,
                market_mood_score = :market_mood_score,
                message = :message,
                error = :error
            WHERE run_uid = :uid
        """, {
            "uid": run_uid,
            "status": status,
            "trade_date": payload.get("trade_date"),
            "progress_percent": progress_percent,
            "done_count": done_count,
            "total": payload.get("total"),
            "passed": payload.get("passed"),
            "flow_date": str(payload.get("flow_date") or "")[:20],
            "hot_date": str(payload.get("hot_date") or "")[:20],
            "market_mood_score": payload.get("market_mood_score"),
            "message": str(payload.get("message") or "")[:500],
            "error": str(payload.get("error") or "")[:500],
        })
    except Exception as exc:
        _record_fallback('_recommended_run_history_finish:8334', exc)


def _recommended_run_history_expire_stale(max_age_minutes: int = 180) -> None:
    try:
        minutes = max(5, min(1440, int(max_age_minutes)))
    except (TypeError, ValueError):
        minutes = 180
    try:
        _ensure_recommended_run_history_table()
        _exec_sql(f"""
            UPDATE st_recommended_run_history
            SET status = 'error',
                finished_at = NOW(),
                duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                progress_percent = 0,
                message = '任务已中断或超时，已自动清理，避免生产卡死',
                error = 'stale running recommendation job expired'
            WHERE status = 'running'
              AND started_at < DATE_SUB(NOW(), INTERVAL {minutes} MINUTE)
        """, {})
    except Exception as exc:
        _record_fallback('_recommended_run_history_expire_stale', exc)


def _web_ai_recommendation_run_enabled() -> bool:
    return os.environ.get("PROBIGA_ENABLE_WEB_AI_RECOMMENDATION_RUN", "").strip() == "1"


def _web_ai_recommendation_queue_enabled() -> bool:
    return os.environ.get("PROBIGA_ENABLE_WEB_AI_RECOMMENDATION_QUEUE", "1").strip() != "0"


def _web_ai_recommendation_queue_autostart_enabled() -> bool:
    return os.environ.get("PROBIGA_ENABLE_WEB_AI_RECOMMENDATION_QUEUE_AUTOSTART", "1").strip() != "0"


def _web_ai_recommendation_queue_worker_allow_intraday() -> bool:
    return os.environ.get("PROBIGA_AI_RECOMMEND_QUEUE_WORKER_ALLOW_INTRADAY", "1").strip() != "0"


def _start_recommended_queue_worker(*, run_uid: str, refresh_realtime: bool) -> dict:
    from server.api.scheduler_runtime import start_detached_python_job

    cmd = [
        sys.executable,
        str(_ROOT / "tools" / "run_ai_recommendation_worker.py"),
        "--once",
        "--force",
    ]
    if refresh_realtime:
        cmd.append("--refresh-realtime")
    if _web_ai_recommendation_queue_worker_allow_intraday():
        cmd.append("--allow-intraday")
    cmd.extend([
        "--max-load",
        os.environ.get(
            "PROBIGA_AI_RECOMMEND_QUEUE_WORKER_MAX_LOAD",
            os.environ.get("PROBIGA_AI_WORKER_MAX_LOAD", "8.00"),
        ),
        "--min-memory-mb",
        os.environ.get(
            "PROBIGA_AI_RECOMMEND_QUEUE_WORKER_MIN_MEMORY_MB",
            os.environ.get("PROBIGA_AI_WORKER_MIN_MEMORY_MB", "700"),
        ),
    ])

    env = build_child_env(_ROOT, engine=get_engine())
    env.update({
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "PROBIGA_AI_RECOMMEND_WORKER_ENABLED": "1",
    })
    return start_detached_python_job(
        cmd=cmd,
        root=_ROOT,
        env=env,
        log_name=f"recommended_queue_worker_{run_uid}",
        nice=10,
    )


def _protected_recommended_run_response(
    *,
    trade_date: str,
    min_score: float,
    top_n: int,
    strict_prev_trade_day: bool,
    execution_time: str,
    run_context: dict | None = None,
) -> dict:
    message = "生产保护模式：Web 端全市场 AI 推荐已暂停，避免 2核4G 服务器再次卡死；请改用独立 worker 或低峰手工任务生成。"
    progress = {
        "status": "protected",
        "percent": 0,
        "step": message,
        "total": 0,
        "done": 0,
        "passed": 0,
        "trade_date": trade_date,
        "min_score": float(min_score),
        "top_n": int(top_n),
        "strict_prev_trade_day": bool(strict_prev_trade_day),
        "execution_time": execution_time,
        "run_context": run_context or {},
        "is_running": False,
    }
    _cache_set("rec_screen_progress", progress)
    return {
        "status": "protected",
        "date": trade_date,
        "min_score": float(min_score),
        "top_n": int(top_n),
        "strict_prev_trade_day": bool(strict_prev_trade_day),
        "execution_time": execution_time,
        "run_context": run_context or {},
        "progress": progress,
        "note": message,
    }


def _queued_recommended_run_response(
    *,
    trade_date: str,
    min_score: float,
    top_n: int,
    strict_prev_trade_day: bool,
    execution_time: str,
    refresh_realtime: bool = True,
    run_context: dict | None = None,
) -> dict:
    message = "AI 推荐任务已入队；将由独立低优先级 worker 后台执行，避免生产 API 卡死。"
    run_uid = _recommended_run_history_start(
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        strict_prev_trade_day=strict_prev_trade_day,
        execution_time=execution_time,
        message=message,
        status="queued",
    )
    worker_info = None
    worker_error = ""
    if run_uid and _web_ai_recommendation_queue_autostart_enabled():
        try:
            worker_info = _start_recommended_queue_worker(
                run_uid=run_uid,
                refresh_realtime=bool(refresh_realtime),
            )
            message = "AI 推荐任务已入队，worker 已自动启动；正在等待后台领取任务。"
            _recommended_run_history_update(run_uid, status="queued", payload={
                "progress_percent": 5,
                "done_count": 0,
                "message": message,
                "error": "",
            })
        except Exception as exc:
            worker_error = str(exc)[:500]
            message = "AI 推荐任务已入队，但自动启动 worker 失败；请检查后台 worker。"
            _recommended_run_history_update(run_uid, status="queued", payload={
                "progress_percent": 5,
                "done_count": 0,
                "message": message,
                "error": worker_error,
            })
    progress = {
        "status": "queued",
        "percent": 5,
        "step": message,
        "total": 0,
        "done": 0,
        "passed": 0,
        "trade_date": trade_date,
        "min_score": float(min_score),
        "top_n": int(top_n),
        "strict_prev_trade_day": bool(strict_prev_trade_day),
        "execution_time": execution_time,
        "refresh_realtime": bool(refresh_realtime),
        "use_intraday_current": bool((run_context or {}).get("use_intraday_current")),
        "run_context": run_context or {},
        "run_uid": run_uid,
        "is_running": True,
    }
    if worker_info:
        progress["worker"] = worker_info
    if worker_error:
        progress["worker_error"] = worker_error
    _cache_set("rec_screen_progress", progress)
    return {
        "status": "queued",
        "date": trade_date,
        "min_score": float(min_score),
        "top_n": int(top_n),
        "strict_prev_trade_day": bool(strict_prev_trade_day),
        "execution_time": execution_time,
        "refresh_realtime": bool(refresh_realtime),
        "use_intraday_current": bool((run_context or {}).get("use_intraday_current")),
        "run_context": run_context or {},
        "run_uid": run_uid,
        "progress": progress,
        "worker": worker_info,
        "worker_error": worker_error,
        "note": message,
    }


def _recommended_history_progress(run_uid: str = "") -> dict | None:
    try:
        _ensure_recommended_run_history_table()
        if run_uid:
            rows = _read_sql("""
                SELECT run_uid, trade_date, status, min_score, top_n, strict_prev_trade_day,
                        execution_time, started_at, finished_at, duration_seconds,
                        progress_percent, done_count, total, passed,
                        flow_date, hot_date, market_mood_score, message, error
                FROM st_recommended_run_history
                WHERE run_uid = :run_uid
                LIMIT 1
            """, {"run_uid": run_uid})
        else:
            rows = _read_sql("""
                SELECT run_uid, trade_date, status, min_score, top_n, strict_prev_trade_day,
                        execution_time, started_at, finished_at, duration_seconds,
                        progress_percent, done_count, total, passed,
                        flow_date, hot_date, market_mood_score, message, error
                FROM st_recommended_run_history
                ORDER BY started_at DESC, id DESC
                LIMIT 1
            """, {})
        if not rows:
            return None
        row = rows[0]
        status = str(row.get("status") or "idle")
        is_running = status == "running"
        is_active = status in {"running", "queued"}
        fallback_percent = 20 if is_running else (5 if status == "queued" else (100 if status == "done" else 0))
        try:
            percent = int(row.get("progress_percent") if row.get("progress_percent") is not None else fallback_percent)
        except (TypeError, ValueError):
            percent = fallback_percent
        percent = max(0, min(100, percent))
        step = str(row.get("message") or row.get("error") or "")
        if not step:
            step = "离线推荐任务运行中" if is_running else ("离线推荐任务排队中" if status == "queued" else status)
        done_count = row.get("done_count")
        if done_count is None and status == "done":
            done_count = row.get("total")
        return {
            "status": status,
            "percent": percent,
            "step": step,
            "total": int(row.get("total") or 0),
            "done": int(done_count or 0),
            "passed": int(row.get("passed") or 0),
            "trade_date": str(row.get("trade_date") or "")[:10],
            "min_score": float(row.get("min_score") or 0),
            "top_n": int(row.get("top_n") or 0),
            "strict_prev_trade_day": bool(row.get("strict_prev_trade_day")),
            "execution_time": str(row.get("execution_time") or "")[:19],
            "flow_date": str(row.get("flow_date") or ""),
            "hot_date": str(row.get("hot_date") or ""),
            "market_mood_score": row.get("market_mood_score"),
            "run_uid": str(row.get("run_uid") or ""),
            "started_at": str(row.get("started_at") or "")[:19],
            "finished_at": str(row.get("finished_at") or "")[:19],
            "duration_seconds": row.get("duration_seconds"),
            "error": str(row.get("error") or ""),
            "is_running": is_active,
            "is_active": is_active,
        }
    except Exception as exc:
        _record_fallback('_recommended_history_progress', exc)
        return None


def _queued_recommended_refresh_realtime(progress: dict) -> bool:
    if bool(progress.get("strict_prev_trade_day")):
        return False
    trade_date = str(progress.get("trade_date") or "")[:10]
    execution_time = str(progress.get("execution_time") or "")[:19]
    if not trade_date or not execution_time:
        return False
    dt = _parse_recommendation_execution_time(execution_time)
    if trade_date != dt.date().isoformat():
        return False
    hhmm = dt.hour * 100 + dt.minute
    return 925 <= hhmm <= 1505


def _maybe_autostart_queued_recommended_worker(progress: dict | None) -> None:
    if not progress or str(progress.get("status") or "") != "queued":
        return
    if not _web_ai_recommendation_queue_autostart_enabled():
        return
    run_uid = str(progress.get("run_uid") or "")
    if not run_uid:
        return
    cache_key = f"rec_queue_worker_autostart_{run_uid}"
    if _cache_get(cache_key, ttl_seconds=60):
        return
    _cache_set(cache_key, True)
    refresh_realtime = _queued_recommended_refresh_realtime(progress)
    try:
        worker_info = _start_recommended_queue_worker(
            run_uid=run_uid,
            refresh_realtime=refresh_realtime,
        )
        _recommended_run_history_update(run_uid, status="queued", payload={
            "progress_percent": 5,
            "message": "queued AI recommendation worker auto-retried",
            "error": "",
        })
        progress["worker"] = worker_info
    except Exception as exc:
        _recommended_run_history_update(run_uid, status="queued", payload={
            "message": "queued AI recommendation worker auto-retry failed",
            "error": str(exc)[:500],
        })


@router.get("/hot-data/recommended-stocks/progress")
def recommended_stocks_progress():
    """查询 AI 推荐筛选进度"""
    _recommended_run_history_expire_stale()
    cached = _cache_get("rec_screen_progress", ttl_seconds=7200)
    if cached is not None:
        if isinstance(cached, dict) and str(cached.get("status") or "") == "protected":
            return {**cached, "is_running": False}
        cached_run_uid = str(cached.get("run_uid") or "") if isinstance(cached, dict) else ""
        history = _recommended_history_progress(cached_run_uid) if cached_run_uid else None
        latest_history = _recommended_history_progress()
        if (
            latest_history
            and latest_history.get("run_uid")
            and latest_history.get("run_uid") != cached_run_uid
        ):
            return latest_history
        if history and history.get("run_uid"):
            history_status = str(history.get("status") or "")
            cached_status = str(cached.get("status") or "") if isinstance(cached, dict) else ""
            cached_percent = int(cached.get("percent") or 0) if isinstance(cached, dict) else 0
            history_percent = int(history.get("percent") or 0)
            if history_status in {"done", "error", "protected"}:
                return history
            if history_status == "queued":
                _maybe_autostart_queued_recommended_worker(history)
                return history
            if cached_status != history_status:
                return history
            if history_status == "running" and history_percent >= cached_percent:
                return history
        return {
            **cached,
            "is_running": (
                _job_is_running("recommended_stocks")
                or bool(history and history.get("is_running"))
                or (isinstance(cached, dict) and str(cached.get("status") or "") == "queued")
            ),
        }
    history = _recommended_history_progress()
    if history and (history.get("is_running") or history.get("status") in {"queued", "protected"}):
        if str(history.get("status") or "") == "queued":
            _maybe_autostart_queued_recommended_worker(history)
        return history
    return {"status": "idle", "percent": 0, "step": "未启动", "is_running": False}


@router.get("/hot-data/recommended-stocks/run-history")
def recommended_stocks_run_history(limit: int = Query(default=10, ge=1, le=50)):
    """查询 AI 推荐筛选执行历史"""
    try:
        _recommended_run_history_expire_stale()
        _ensure_recommended_run_history_table()
        rows = _read_sql("""
            SELECT id, run_uid, trade_date, status, min_score, top_n, strict_prev_trade_day,
                   execution_time, started_at, finished_at, duration_seconds,
                   total, passed, flow_date, hot_date, market_mood_score, message, error
            FROM st_recommended_run_history
            ORDER BY started_at DESC, id DESC
            LIMIT :limit
        """, {"limit": limit})
        return {"data": rows, "total": len(rows)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


def _active_recommended_run() -> dict | None:
    try:
        _recommended_run_history_expire_stale()
        _ensure_recommended_run_history_table()
        rows = _read_sql("""
            SELECT run_uid, trade_date, status, min_score, top_n, strict_prev_trade_day,
                   execution_time, started_at, message
            FROM st_recommended_run_history
            WHERE status IN ('running', 'queued')
              AND started_at >= DATE_SUB(NOW(), INTERVAL 12 HOUR)
            ORDER BY started_at DESC, id DESC
            LIMIT 1
        """, {})
        return rows[0] if rows else None
    except Exception as exc:
        _record_fallback('_active_recommended_run', exc)
        return None


def _start_recommended_offline_process(
    *,
    run_uid: str,
    trade_date: str,
    min_score: float,
    top_n: int,
    strict_prev_trade_day: bool,
    execution_time: str,
    min_kline_coverage: float,
    auto_repair_missing_kline: bool,
    refresh_realtime: bool,
    use_intraday_current: bool = False,
) -> dict:
    from server.api.scheduler_runtime import start_detached_python_job

    cmd = [
        sys.executable,
        str(_ROOT / "tools" / "run_ai_recommendation_premarket.py"),
        "--date",
        trade_date,
        "--top-n",
        str(int(top_n)),
        "--min-score",
        str(float(min_score)),
        "--execution-time",
        execution_time,
        "--min-kline-coverage",
        str(float(min_kline_coverage)),
        "--run-uid",
        run_uid,
        "--json",
    ]
    if strict_prev_trade_day:
        cmd.append("--strict-prev-trade-day")
    if auto_repair_missing_kline:
        cmd.append("--auto-repair-missing-kline")
        cmd.append("--auto-repair-missing-data")
    if refresh_realtime:
        cmd.append("--refresh-realtime")
    if use_intraday_current:
        cmd.append("--use-intraday-current")

    env = build_child_env(_ROOT, engine=get_engine())
    env.update({
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
    })
    return start_detached_python_job(
        cmd=cmd,
        root=_ROOT,
        env=env,
        log_name=f"recommended_stocks_{run_uid}",
        nice=10,
    )


def _run_recommended_batch_in_process(
    *,
    engine,
    trade_date: str,
    top_n: int,
    min_score: float,
    progress_callback,
    strict_prev_trade_day: bool,
    execution_time: str,
    min_kline_coverage: float,
    auto_repair_missing_kline: bool,
    use_intraday_current: bool,
):
    """Thread fallback that preserves the caller's one exact run cutoff."""
    from biz.analysis.sync_analysis_fast import run_batch

    return run_batch(
        engine=engine,
        trade_date=trade_date,
        top_n=top_n,
        min_score=float(min_score),
        progress_callback=progress_callback,
        strict_prev_trade_day=bool(strict_prev_trade_day),
        execution_time=execution_time or None,
        min_kline_coverage=float(min_kline_coverage),
        auto_repair_missing_kline=bool(auto_repair_missing_kline),
        use_intraday_current=bool(use_intraday_current),
    )


@router.post("/hot-data/recommended-stocks/run")
def run_recommended_stocks(
    trade_date: str = Query(default=""),
    min_score: float = Query(default=62.0, ge=0, le=100),
    top_n: int = Query(default=80, ge=20, le=200),
    strict_prev_trade_day: bool = Query(default=False),
    execution_time: str = Query(default=""),
    min_kline_coverage: float = Query(default=0.80, ge=0, le=1),
    auto_repair_missing_kline: bool = Query(default=True),
    refresh_realtime: bool = Query(default=True),
    date_policy: str = Query(default="auto"),
):
    """触发 AI 推荐股票筛选（使用统一分析引擎）"""
    try:
        run_context = _resolve_recommended_run_context(
            trade_date=trade_date,
            strict_prev_trade_day=bool(strict_prev_trade_day),
            execution_time=execution_time,
            refresh_realtime=bool(refresh_realtime),
            date_policy=date_policy,
        )
    except Exception as exc:
        return {
            "status": "error",
            "date": str(trade_date or "")[:10],
            "min_score": min_score,
            "top_n": top_n,
            "strict_prev_trade_day": bool(strict_prev_trade_day),
            "error": str(exc),
        }

    trade_date = str(run_context.get("trade_date") or "")[:10]
    strict_prev_trade_day = bool(run_context.get("strict_prev_trade_day"))
    execution_time = str(run_context.get("execution_time") or "")
    refresh_realtime = bool(run_context.get("refresh_realtime"))
    use_intraday_current = bool(run_context.get("use_intraday_current"))

    _recommended_run_history_expire_stale()
    active = _active_recommended_run()
    if active:
        progress = _recommended_history_progress(str(active.get("run_uid") or "")) or recommended_stocks_progress()
        active_status = str(active.get("status") or progress.get("status") or "running")
        process_info = None
        worker_error = ""
        if active_status == "queued" and _web_ai_recommendation_queue_autostart_enabled():
            try:
                process_info = _start_recommended_queue_worker(
                    run_uid=str(active.get("run_uid") or ""),
                    refresh_realtime=bool(refresh_realtime),
                )
                _recommended_run_history_update(str(active.get("run_uid") or ""), status="queued", payload={
                    "progress_percent": 5,
                    "message": "queued AI recommendation worker restarted",
                    "error": "",
                })
                progress = _recommended_history_progress(str(active.get("run_uid") or "")) or progress
            except Exception as exc:
                worker_error = str(exc)[:500]
                _recommended_run_history_update(str(active.get("run_uid") or ""), status="queued", payload={
                    "message": "queued AI recommendation worker restart failed",
                    "error": worker_error,
                })
        return {
            "status": active_status,
            "date": str(active.get("trade_date") or "")[:10],
            "min_score": float(active.get("min_score") or min_score),
            "top_n": int(active.get("top_n") or top_n),
            "strict_prev_trade_day": bool(active.get("strict_prev_trade_day")),
            "run_uid": str(active.get("run_uid") or ""),
            "progress": progress,
            "process": process_info,
            "worker_error": worker_error,
            "run_context": run_context,
            "note": "已有 AI 推荐任务在队列或后台运行",
        }

    strict_gate = None
    if strict_prev_trade_day and not auto_repair_missing_kline:
        try:
            strict_gate = _recommendation_gate_status(
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
                target_trade_date=trade_date,
            )
            if not strict_gate.get("ready"):
                gate_error = strict_gate.get("error") or "target trade date data is not ready"
                return {
                    "status": "error",
                    "date": trade_date,
                    "min_score": min_score,
                    "top_n": top_n,
                    "strict_prev_trade_day": True,
                    "gate": strict_gate,
                    "run_context": run_context,
                    "error": gate_error,
                }
        except Exception as exc:
            return {
                "status": "error",
                "date": trade_date,
                "min_score": min_score,
                "top_n": top_n,
                "strict_prev_trade_day": True,
                "run_context": run_context,
                "error": str(exc),
            }

    if _web_ai_recommendation_queue_enabled():
        return _queued_recommended_run_response(
            trade_date=trade_date,
            min_score=min_score,
            top_n=top_n,
            strict_prev_trade_day=strict_prev_trade_day,
            execution_time=execution_time,
            refresh_realtime=refresh_realtime,
            run_context=run_context,
        )
    return _protected_recommended_run_response(
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        strict_prev_trade_day=strict_prev_trade_day,
        execution_time=execution_time,
        run_context=run_context,
    )

    import threading

    if os.environ.get("PROBIGA_ALLOW_INLINE_AI_RECOMMENDATION_RUN", "").strip() != "1":
        _recommended_run_history_expire_stale()
        active = _active_recommended_run()
        if active:
            progress = _recommended_history_progress(str(active.get("run_uid") or "")) or recommended_stocks_progress()
            active_status = str(active.get("status") or progress.get("status") or "running")
            process_info = None
            worker_error = ""
            if active_status == "queued" and _web_ai_recommendation_queue_autostart_enabled():
                try:
                    process_info = _start_recommended_queue_worker(
                        run_uid=str(active.get("run_uid") or ""),
                        refresh_realtime=bool(refresh_realtime),
                    )
                    _recommended_run_history_update(str(active.get("run_uid") or ""), status="queued", payload={
                        "progress_percent": 5,
                        "message": "已有 AI 推荐任务在排队，worker 已重新唤起。",
                        "error": "",
                    })
                    progress = _recommended_history_progress(str(active.get("run_uid") or "")) or progress
                except Exception as exc:
                    worker_error = str(exc)[:500]
                    _recommended_run_history_update(str(active.get("run_uid") or ""), status="queued", payload={
                        "message": "已有 AI 推荐任务在排队，但自动唤起 worker 失败。",
                        "error": worker_error,
                    })
            return {
                "status": active_status,
                "date": str(active.get("trade_date") or "")[:10],
                "min_score": float(active.get("min_score") or min_score),
                "top_n": int(active.get("top_n") or top_n),
                "strict_prev_trade_day": bool(active.get("strict_prev_trade_day")),
                "run_uid": str(active.get("run_uid") or ""),
                "progress": progress,
                "process": process_info,
                "worker_error": worker_error,
                "note": "已有 AI 推荐任务在排队" if active_status == "queued" else "已有离线 AI 推荐任务在运行",
            }

        strict_gate = None
        if strict_prev_trade_day:
            try:
                strict_gate = _recommendation_gate_status(
                    execution_time=execution_time,
                    min_kline_coverage=min_kline_coverage,
                )
                trade_date = strict_gate["expected_trade_date"]
                execution_time = strict_gate["execution_time"]
                if not strict_gate.get("ready") and not auto_repair_missing_kline:
                    gate_error = strict_gate.get("error") or "目标日基础数据未就绪"
                    return {
                        "status": "error",
                        "date": trade_date,
                        "min_score": min_score,
                        "top_n": top_n,
                        "strict_prev_trade_day": True,
                        "gate": strict_gate,
                        "error": gate_error,
                    }
            except Exception as exc:
                return {
                    "status": "error",
                    "date": str(trade_date or "")[:10],
                    "min_score": min_score,
                    "top_n": top_n,
                    "strict_prev_trade_day": True,
                    "error": str(exc),
                }
        elif not trade_date:
            trade_date = _smart_trade_date()
        trade_date = str(trade_date or "")[:10]
        execution_time = execution_time or datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        if not _web_ai_recommendation_run_enabled():
            if _web_ai_recommendation_queue_enabled():
                return _queued_recommended_run_response(
                    trade_date=trade_date,
                    min_score=min_score,
                    top_n=top_n,
                    strict_prev_trade_day=strict_prev_trade_day,
                    execution_time=execution_time,
                    refresh_realtime=refresh_realtime,
                )
            return _protected_recommended_run_response(
                trade_date=trade_date,
                min_score=min_score,
                top_n=top_n,
                strict_prev_trade_day=strict_prev_trade_day,
                execution_time=execution_time,
            )
        run_uid = _recommended_run_history_start(
            trade_date=trade_date,
            min_score=min_score,
            top_n=top_n,
            strict_prev_trade_day=strict_prev_trade_day,
            execution_time=execution_time,
            message="离线 AI 推荐任务已启动；先刷新点击时实时数据",
        )
        _set_recommendation_progress(
            status="running",
            percent=0,
            step="离线 AI 推荐任务已启动；正在刷新点击时实时数据",
            total=0,
            done=0,
            passed=0,
            trade_date=trade_date,
            min_score=min_score,
            top_n=top_n,
            strict_prev_trade_day=strict_prev_trade_day,
            execution_time=execution_time,
            min_kline_coverage=min_kline_coverage,
            auto_repair_missing_kline=auto_repair_missing_kline,
            refresh_realtime=refresh_realtime,
            gate=strict_gate,
            run_uid=run_uid,
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        try:
            process_info = _start_recommended_offline_process(
                run_uid=run_uid,
                trade_date=trade_date,
                min_score=min_score,
                top_n=top_n,
                strict_prev_trade_day=strict_prev_trade_day,
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
                auto_repair_missing_kline=auto_repair_missing_kline,
                refresh_realtime=refresh_realtime,
                use_intraday_current=use_intraday_current,
            )
        except Exception as exc:
            _recommended_run_history_finish(run_uid, status="error", payload={
                "trade_date": trade_date,
                "message": "离线 AI 推荐任务启动失败",
                "error": str(exc)[:500],
            })
            _set_recommendation_progress(
                status="error",
                percent=0,
                step=f"离线任务启动失败: {str(exc)[:80]}",
                trade_date=trade_date,
                run_uid=run_uid,
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return {
                "status": "error",
                "date": trade_date,
                "min_score": min_score,
                "top_n": top_n,
                "strict_prev_trade_day": strict_prev_trade_day,
                "run_uid": run_uid,
                "error": str(exc),
            }
        return {
            "status": "started",
            "date": trade_date,
            "min_score": min_score,
            "top_n": top_n,
            "strict_prev_trade_day": strict_prev_trade_day,
            "execution_time": execution_time,
            "min_kline_coverage": min_kline_coverage,
            "auto_repair_missing_kline": auto_repair_missing_kline,
            "refresh_realtime": refresh_realtime,
            "gate": strict_gate,
            "run_uid": run_uid,
            "process": process_info,
            "progress": recommended_stocks_progress(),
            "note": "离线 AI 推荐任务已启动，完成后页面会读取今天的新结果",
        }

    strict_gate = None
    if strict_prev_trade_day:
        try:
            strict_gate = _recommendation_gate_status(
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
            )
            trade_date = strict_gate["expected_trade_date"]
            execution_time = strict_gate["execution_time"]
            if not strict_gate.get("ready") and not auto_repair_missing_kline:
                gate_error = strict_gate.get("error") or "目标日基础数据未就绪"
                run_uid = _recommended_run_history_start(
                    trade_date=trade_date,
                    min_score=min_score,
                    top_n=top_n,
                    strict_prev_trade_day=True,
                    execution_time=execution_time,
                    message="严格门禁未通过",
                )
                _recommended_run_history_finish(run_uid, status="error", payload={
                    "message": "严格门禁未通过",
                    "error": gate_error,
                })
                _set_recommendation_progress(
                    status="error",
                    percent=0,
                    step=f"严格门禁未通过：{gate_error}",
                    total=0,
                    done=0,
                    passed=0,
                    trade_date=trade_date,
                    min_score=min_score,
                    top_n=top_n,
                    strict_prev_trade_day=True,
                    auto_repair_missing_kline=False,
                    gate=strict_gate,
                    run_uid=run_uid,
                    finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                return {
                    "status": "error",
                    "date": trade_date,
                    "min_score": min_score,
                    "top_n": top_n,
                    "strict_prev_trade_day": True,
                    "auto_repair_missing_kline": False,
                    "gate": strict_gate,
                    "run_uid": run_uid,
                    "error": gate_error,
                }
        except Exception as exc:
            return {
                "status": "error",
                "date": trade_date,
                "min_score": min_score,
                "top_n": top_n,
                "strict_prev_trade_day": True,
                "error": str(exc),
            }
    elif not trade_date:
        trade_date = _smart_trade_date()
    trade_date = str(trade_date)[:10]

    if not _job_begin("recommended_stocks"):
        return {
            "status": "running",
            "date": trade_date,
            "min_score": min_score,
            "top_n": top_n,
            "strict_prev_trade_day": strict_prev_trade_day,
            "auto_repair_missing_kline": auto_repair_missing_kline,
            "gate": strict_gate,
            "progress": recommended_stocks_progress(),
            "note": "已有推荐筛选任务在运行",
        }

    initial_step = "正在初始化..."
    if strict_prev_trade_day and strict_gate and not strict_gate.get("ready") and auto_repair_missing_kline:
        initial_step = f"目标日 {trade_date} 数据未就绪，后台先用国金QMT补K线..."
    run_uid = _recommended_run_history_start(
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        strict_prev_trade_day=strict_prev_trade_day,
        execution_time=execution_time,
        message=initial_step,
    )

    _set_recommendation_progress(
        status="running",
        percent=0,
        step=initial_step,
        total=0,
        done=0,
        passed=0,
        trade_date=trade_date,
        min_score=min_score,
        top_n=top_n,
        strict_prev_trade_day=strict_prev_trade_day,
        execution_time=execution_time,
        min_kline_coverage=min_kline_coverage,
        auto_repair_missing_kline=auto_repair_missing_kline,
        gate=strict_gate,
        run_uid=run_uid,
        started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    def _run_screen():
        try:
            engine = get_engine()

            def _progress_callback(event: dict):
                stage = str(event.get("stage") or "")
                if stage == "done":
                    _set_recommendation_progress(
                        status="done",
                        percent=100,
                        step=f"V3筛选完成，通过 {int(event.get('recommendation_count') or 0)} 只",
                        total=int(event.get("analysis_count") or 0),
                        done=int(event.get("analysis_count") or 0),
                        passed=int(event.get("recommendation_count") or 0),
                        trade_date=trade_date,
                        min_score=min_score,
                        top_n=top_n,
                        strict_prev_trade_day=strict_prev_trade_day,
                        execution_time=execution_time,
                        min_kline_coverage=min_kline_coverage,
                        auto_repair_missing_kline=auto_repair_missing_kline,
                        gate=strict_gate,
                        run_uid=run_uid,
                        flow_date=event.get("flow_date") or "",
                        hot_date=event.get("hot_date") or "",
                        market_mood_score=event.get("market_mood_score"),
                        finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    return
                _set_recommendation_progress(
                    status="running",
                    percent=int(event.get("percent") or 0),
                    step=str(event.get("step") or "运行中..."),
                    total=int(event.get("analysis_count") or 0),
                    done=int(event.get("done") or 0),
                    passed=int(event.get("recommendation_count") or 0),
                    trade_date=trade_date,
                    min_score=min_score,
                    top_n=top_n,
                    strict_prev_trade_day=strict_prev_trade_day,
                    execution_time=execution_time,
                    min_kline_coverage=min_kline_coverage,
                    auto_repair_missing_kline=auto_repair_missing_kline,
                    run_uid=run_uid,
                    )

            stats = _run_recommended_batch_in_process(
                engine=engine,
                trade_date=trade_date,
                top_n=top_n,
                min_score=min_score,
                progress_callback=_progress_callback,
                strict_prev_trade_day=strict_prev_trade_day,
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
                auto_repair_missing_kline=auto_repair_missing_kline,
                use_intraday_current=use_intraday_current,
            )
            _set_recommendation_progress(
                status="done",
                percent=100,
                step=f"V3筛选完成，通过 {stats.recommendation_count} 只",
                total=stats.analysis_count,
                done=stats.analysis_count,
                passed=stats.recommendation_count,
                trade_date=trade_date,
                min_score=min_score,
                top_n=top_n,
                strict_prev_trade_day=strict_prev_trade_day,
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
                auto_repair_missing_kline=auto_repair_missing_kline,
                gate=strict_gate,
                run_uid=run_uid,
                flow_date=stats.flow_date,
                hot_date=stats.hot_date,
                market_mood_score=stats.market_mood_score,
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            _recommended_run_history_finish(run_uid, status="done", payload={
                "trade_date": stats.trade_date,
                "total": stats.analysis_count,
                "passed": stats.recommendation_count,
                "flow_date": stats.flow_date,
                "hot_date": stats.hot_date,
                "market_mood_score": stats.market_mood_score,
                "message": f"V3筛选完成，通过 {stats.recommendation_count} 只",
            })
            try:
                _invalidate_recommended_stocks_cache()
                default_result = _recommended_stocks_v2(trade_date, "", "")
                _cache_set(f"recommended_stocks_{trade_date}_all_all", default_result)
                _cache_set("recommended_stocks_latest_all_all", default_result)
            except Exception as exc:
                _record_fallback('_run_screen:8578', exc)
        except Exception as ex:
            import logging
            logging.getLogger(__name__).error(f"recommended-stocks error: {ex}", exc_info=True)
            _set_recommendation_progress(
                status="error",
                percent=0,
                step=f"筛选失败: {str(ex)[:80]}",
                total=0,
                done=0,
                passed=0,
                trade_date=trade_date,
                min_score=min_score,
                top_n=top_n,
                strict_prev_trade_day=strict_prev_trade_day,
                execution_time=execution_time,
                min_kline_coverage=min_kline_coverage,
                auto_repair_missing_kline=auto_repair_missing_kline,
                gate=strict_gate,
                run_uid=run_uid,
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            _recommended_run_history_finish(run_uid, status="error", payload={
                "message": "筛选失败",
                "error": str(ex)[:500],
            })
        finally:
            _job_end("recommended_stocks")

    t = threading.Thread(target=_run_screen, daemon=True)
    t.start()
    return {
        "status": "started",
        "date": trade_date,
        "min_score": min_score,
        "top_n": top_n,
        "strict_prev_trade_day": strict_prev_trade_day,
        "execution_time": execution_time,
        "min_kline_coverage": min_kline_coverage,
        "auto_repair_missing_kline": auto_repair_missing_kline,
        "gate": strict_gate,
        "run_uid": run_uid,
        "note": "筛选已启动，完成后刷新页面查看结果",
    }


# ──────────────────────────────────────────────────────────────────────
#  主力行为分析 (建仓 / 洗盘 / 出货)
# ──────────────────────────────────────────────────────────────────────

def _compute_mainforce_behavior_fast(stock_code: str, trade_date: str = None) -> dict:
    """Fast mainforce view based on snapshot + recent capital flow.

    This path avoids sm_stock_kline, which is frequently blocked by metadata locks
    during ingestion and should not sit on the interactive UI critical path.
    """
    requested_trade_date = str(trade_date or date.today().isoformat()).strip()[:10]
    trade_date = _latest_market_analysis_date(requested_trade_date)
    name_rows = _read_sql("SELECT short_name FROM si_all_code WHERE stock_code = :c", {"c": stock_code})
    short_name = name_rows[0]["short_name"] if name_rows else stock_code

    snapshot_rows = _read_sql("""
        SELECT stock_code, short_name, trade_date, price, change_pct, change_3d, change_5d, change_10d,
               turnover_ratio, amount, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow
        FROM sm_stock_snapshot
        WHERE stock_code = :c AND trade_date = :td
        LIMIT 1
    """, {"c": stock_code, "td": trade_date})
    snapshot = snapshot_rows[0] if snapshot_rows else {}
    if snapshot.get("short_name"):
        short_name = snapshot.get("short_name")

    data_basis = "行情快照"
    if not snapshot:
        kline_rows = _read_sql("""
            SELECT trade_date, close, amount, change_pct, turnover_ratio
            FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1 AND trade_date <= :td
            ORDER BY trade_date DESC
            LIMIT 11
        """, {"c": stock_code, "td": trade_date})
        klines = list(reversed(kline_rows)) if kline_rows else []
        if klines:
            latest_k = klines[-1]

            def _period_change(days: int) -> float:
                if len(klines) <= days:
                    return 0.0
                base = float(klines[-1 - days].get("close") or 0)
                close = float(latest_k.get("close") or 0)
                return (close / base - 1.0) * 100 if base > 0 and close > 0 else 0.0

            snapshot = {
                "stock_code": stock_code,
                "short_name": short_name,
                "trade_date": str(latest_k.get("trade_date") or trade_date)[:10],
                "price": latest_k.get("close"),
                "change_pct": latest_k.get("change_pct"),
                "change_3d": _period_change(3),
                "change_5d": _period_change(5),
                "change_10d": _period_change(10),
                "turnover_ratio": latest_k.get("turnover_ratio"),
                "amount": latest_k.get("amount"),
                "main_net_inflow": 0,
                "max_net_inflow": 0,
                "lg_net_inflow": 0,
                "mid_net_inflow": 0,
                "sm_net_inflow": 0,
            }
            trade_date = str(snapshot.get("trade_date") or trade_date)[:10]
            data_basis = "日K线"

    flow_rows = _read_sql("""
        SELECT trade_date, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow
        FROM sm_stock_capital_flow_daily
        WHERE stock_code = :c AND trade_date <= :td
        ORDER BY trade_date DESC
        LIMIT 20
    """, {"c": stock_code, "td": trade_date})
    flows = list(reversed(flow_rows)) if flow_rows else []
    flow_dates = [str(r["trade_date"])[:10] for r in flows]
    main_flows = [float(r.get("main_net_inflow") or 0) for r in flows]
    sm_flows = [float(r.get("sm_net_inflow") or 0) for r in flows]

    holder_rows = _read_sql("""
        SELECT holder_num, holder_num_change, holder_num_ratio, avg_free_shares
        FROM si_stock_holder WHERE stock_code = :c
        ORDER BY report_date DESC LIMIT 2
    """, {"c": stock_code})
    holder = holder_rows[0] if holder_rows else {}
    holder_change = float(holder.get("holder_num_ratio") or 0) if holder else 0

    lhb_rows = _read_sql("""
        SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy
        FROM st_a_list_daily
        WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 30 DAY)
    """, {"c": stock_code, "td": trade_date})
    lhb = lhb_rows[0] if lhb_rows else {}
    lhb_count = int(lhb.get("cnt") or 0)
    lhb_net_buy = float(lhb.get("inst_net_buy") or 0)

    price_chg = float(snapshot.get("change_pct") or 0)
    chg3 = float(snapshot.get("change_3d") or 0)
    chg5 = float(snapshot.get("change_5d") or 0)
    chg10 = float(snapshot.get("change_10d") or 0)
    turnover_ratio = float(snapshot.get("turnover_ratio") or 0)
    amount = float(snapshot.get("amount") or 0)
    day_main_flow = float(snapshot.get("main_net_inflow") or (main_flows[-1] if main_flows else 0))

    vp_score = 50
    vp_direction = "中性"
    if day_main_flow > 0 and -2 <= price_chg <= 4:
        vp_score = 72
        vp_direction = "建仓"
        vp_detail = f"当日主力净流入{day_main_flow/1e8:.2f}亿，股价变动{price_chg:+.1f}%，量价配合偏吸筹"
    elif day_main_flow < 0 and price_chg >= 4:
        vp_score = 30
        vp_direction = "出货"
        vp_detail = f"当日主力净流出{abs(day_main_flow)/1e8:.2f}亿，但股价上涨{price_chg:+.1f}%，高位承接偏弱"
    elif price_chg < 0 and turnover_ratio <= 3:
        vp_score = 61
        vp_direction = "洗盘"
        vp_detail = f"当日回调{price_chg:+.1f}%，换手率{turnover_ratio:.2f}%，缩量震荡偏洗盘"
    elif price_chg > 0 and turnover_ratio >= 5 and amount > 1e8:
        vp_score = 64
        vp_direction = "建仓"
        vp_detail = f"当日上涨{price_chg:+.1f}%，换手率{turnover_ratio:.2f}%，成交额{amount/1e8:.1f}亿"
    else:
        vp_detail = f"当日涨跌{price_chg:+.1f}%，换手率{turnover_ratio:.2f}%，暂无强量价背离"

    cf_score = 50
    cf_direction = "中性"
    flow_5d = sum(main_flows[-5:]) if len(main_flows) >= 5 else sum(main_flows)
    flow_10d = sum(main_flows[-10:]) if len(main_flows) >= 10 else sum(main_flows)
    sm_5d = sum(sm_flows[-5:]) if len(sm_flows) >= 5 else sum(sm_flows)
    consecutive_in = 0
    for flow in main_flows[::-1]:
        if flow > 0:
            consecutive_in += 1
        else:
            break
    consecutive_out = 0
    for flow in main_flows[::-1]:
        if flow < 0:
            consecutive_out += 1
        else:
            break
    if flow_5d > 0 and flow_10d >= 0:
        cf_score = 68 + min(12, consecutive_in * 3)
        cf_direction = "建仓"
        cf_detail = f"5日主力净流入{flow_5d/1e8:.2f}亿，10日净流入{flow_10d/1e8:.2f}亿"
    elif flow_5d < 0 and sm_5d > 0:
        cf_score = 24
        cf_direction = "出货"
        cf_detail = f"5日主力净流出{abs(flow_5d)/1e8:.2f}亿，散户净流入{sm_5d/1e8:.2f}亿"
    elif consecutive_out >= 3:
        cf_score = 28
        cf_direction = "出货"
        cf_detail = f"主力连续{consecutive_out}日净流出"
    elif consecutive_in >= 3:
        cf_score = 76
        cf_direction = "建仓"
        cf_detail = f"主力连续{consecutive_in}日净流入"
    else:
        cf_detail = f"5日主力净流入{flow_5d/1e8:.2f}亿，方向暂不明确"

    kl_score = 50
    kl_direction = "中性"
    momentum = chg10 * 0.25 + chg5 * 0.45 + chg3 * 0.3
    if 3 <= chg5 <= 18 and chg10 > 0:
        kl_score = 66
        kl_direction = "建仓"
        kl_detail = f"3/5/10日涨幅为{chg3:+.1f}% / {chg5:+.1f}% / {chg10:+.1f}%，趋势温和上行"
    elif chg3 < 0 and chg10 > 0:
        kl_score = 60
        kl_direction = "洗盘"
        kl_detail = f"短线3日回调{chg3:+.1f}%，但10日仍上涨{chg10:+.1f}%，偏趋势内洗盘"
    elif chg10 < -8:
        kl_score = 30
        kl_direction = "出货"
        kl_detail = f"10日跌幅{chg10:+.1f}%，趋势明显走弱"
    elif chg5 > 20 and day_main_flow < 0:
        kl_score = 34
        kl_direction = "出货"
        kl_detail = f"5日涨幅{chg5:+.1f}%过快，且资金转弱，警惕冲高派发"
    else:
        kl_detail = f"3/5/10日涨幅为{chg3:+.1f}% / {chg5:+.1f}% / {chg10:+.1f}%"

    chip_score = 50
    chip_direction = "中性"
    if holder_change < -5:
        chip_score = 78
        chip_direction = "建仓"
        chip_detail = f"股东人数减少{abs(holder_change):.1f}%，筹码集中明显"
    elif holder_change > 5:
        chip_score = 28
        chip_direction = "出货"
        chip_detail = f"股东人数增加{holder_change:.1f}%，筹码趋于分散"
    elif turnover_ratio <= 2.5 and chg5 >= 0:
        chip_score = 63
        chip_direction = "建仓"
        chip_detail = f"换手率{turnover_ratio:.2f}%偏低，筹码稳定"
    elif turnover_ratio >= 8 and price_chg > 0:
        chip_score = 36
        chip_direction = "出货"
        chip_detail = f"换手率{turnover_ratio:.2f}%偏高，高位换手较剧烈"
    else:
        chip_detail = f"换手率{turnover_ratio:.2f}%，股东变化{holder_change:+.1f}%"

    inst_score = 50
    inst_direction = "中性"
    if lhb_count > 0 and lhb_net_buy > 0:
        inst_score = 72
        inst_direction = "建仓"
        inst_detail = f"近30日{lhb_count}次龙虎榜，机构净买入{lhb_net_buy/1e8:.2f}亿"
    elif lhb_count > 0 and lhb_net_buy < 0:
        inst_score = 26
        inst_direction = "出货"
        inst_detail = f"近30日{lhb_count}次龙虎榜，机构净卖出{abs(lhb_net_buy)/1e8:.2f}亿"
    else:
        inst_detail = "近30日无显著龙虎榜机构信号"

    evidence: list[dict[str, object]] = []

    def _add_evidence(kind: str, direction: str, strength: int, text: str) -> None:
        evidence.append({
            "kind": kind,
            "direction": direction,
            "strength": max(0, min(100, int(strength))),
            "text": text,
        })

    flow_ratio = (day_main_flow / amount * 100) if amount else 0
    if day_main_flow > 0:
        _add_evidence("当日资金", "建仓", 60 + min(30, abs(flow_ratio) * 2), f"主力净流入{day_main_flow/1e8:.2f}亿，占成交额{flow_ratio:.1f}%")
    elif day_main_flow < 0:
        _add_evidence("当日资金", "出货", 60 + min(30, abs(flow_ratio) * 2), f"主力净流出{abs(day_main_flow)/1e8:.2f}亿，占成交额{abs(flow_ratio):.1f}%")
    if consecutive_in >= 3:
        _add_evidence("连续资金", "建仓", 70 + min(20, consecutive_in * 4), f"主力连续{consecutive_in}日净流入")
    if consecutive_out >= 3:
        _add_evidence("连续资金", "出货", 70 + min(20, consecutive_out * 4), f"主力连续{consecutive_out}日净流出")
    if flow_5d < 0 and sm_5d > 0:
        _add_evidence("对手盘", "出货", 82, f"5日主力流出{abs(flow_5d)/1e8:.2f}亿，散户流入{sm_5d/1e8:.2f}亿")
    if chg3 < 0 and chg10 > 0:
        _add_evidence("趋势回撤", "洗盘", 70, f"3日回调{chg3:+.1f}%，10日仍上涨{chg10:+.1f}%")
    if chg5 > 20 and day_main_flow < 0:
        _add_evidence("冲高派发", "出货", 78, f"5日涨幅{chg5:+.1f}%过快且资金转弱")
    if 3 <= chg5 <= 18 and chg10 > 0 and flow_5d >= 0:
        _add_evidence("温和上行", "建仓", 68, f"5日涨幅{chg5:+.1f}%，10日趋势向上且资金未明显撤退")
    if turnover_ratio >= 8 and price_chg > 0:
        _add_evidence("高换手", "出货", 66, f"换手率{turnover_ratio:.2f}%偏高，上涨中承接压力需观察")
    elif turnover_ratio <= 2.5 and chg5 >= 0:
        _add_evidence("筹码稳定", "建仓", 62, f"换手率{turnover_ratio:.2f}%偏低，筹码相对稳定")
    if holder_change < -5:
        _add_evidence("筹码集中", "建仓", 80, f"股东人数减少{abs(holder_change):.1f}%")
    elif holder_change > 5:
        _add_evidence("筹码分散", "出货", 78, f"股东人数增加{holder_change:.1f}%")
    if lhb_count > 0 and lhb_net_buy > 0:
        _add_evidence("龙虎榜", "建仓", 72, f"近30日机构净买入{lhb_net_buy/1e8:.2f}亿")
    elif lhb_count > 0 and lhb_net_buy < 0:
        _add_evidence("龙虎榜", "出货", 72, f"近30日机构净卖出{abs(lhb_net_buy)/1e8:.2f}亿")

    weights = {"volume_price": 0.25, "capital_flow": 0.30, "kline_pattern": 0.20,
               "chip_concentration": 0.15, "institutional": 0.10}
    scores = {
        "volume_price": vp_score,
        "capital_flow": cf_score,
        "kline_pattern": kl_score,
        "chip_concentration": chip_score,
        "institutional": inst_score,
    }
    total_score = round(sum(scores[k] * weights[k] for k in weights), 1)

    direction_votes = {"建仓": 0, "洗盘": 0, "出货": 0, "中性": 0}
    for name, direction in [("volume_price", vp_direction), ("capital_flow", cf_direction),
                             ("kline_pattern", kl_direction), ("chip_concentration", chip_direction),
                             ("institutional", inst_direction)]:
        direction_votes[direction] += weights[name]
    if total_score >= 62:
        behavior = "建仓"
    elif total_score <= 38:
        behavior = "出货"
    elif total_score >= 55:
        behavior = "洗盘" if direction_votes["洗盘"] > direction_votes["出货"] else "建仓"
    elif total_score <= 45:
        behavior = "洗盘" if direction_votes["洗盘"] > direction_votes["建仓"] else "出货"
    else:
        if direction_votes["洗盘"] >= direction_votes["建仓"] and direction_votes["洗盘"] >= direction_votes["出货"]:
            behavior = "洗盘"
        elif direction_votes["建仓"] > direction_votes["出货"]:
            behavior = "建仓"
        else:
            behavior = "出货"
    max_vote = max(direction_votes["建仓"], direction_votes["洗盘"], direction_votes["出货"])
    confidence = round(min(92, max_vote * 100 + 18))

    history = []
    history_source = flows[-10:] if flows else []
    for row in history_source:
        row_flow = float(row.get("main_net_inflow") or 0)
        row_score = 50
        if row_flow > 0:
            row_score = 64
        elif row_flow < 0:
            row_score = 36
        row_behavior = "建仓" if row_score >= 60 else "出货" if row_score <= 40 else "洗盘"
        history.append({"date": str(row.get("trade_date") or "")[:10], "score": row_score, "behavior": row_behavior})
    if not history and trade_date:
        history.append({"date": trade_date, "score": total_score, "behavior": behavior})

    return {
        "stock_code": stock_code,
        "short_name": short_name,
        "trade_date": trade_date,
        "behavior": behavior,
        "confidence": confidence,
        "score": total_score,
        "signals": {
            "volume_price": {"score": round(vp_score), "direction": vp_direction, "detail": vp_detail},
            "capital_flow": {"score": round(cf_score), "direction": cf_direction, "detail": cf_detail},
            "kline_pattern": {"score": round(kl_score), "direction": kl_direction, "detail": kl_detail},
            "chip_concentration": {"score": round(chip_score), "direction": chip_direction, "detail": chip_detail},
            "institutional": {"score": round(inst_score), "direction": inst_direction, "detail": inst_detail},
        },
        "evidence": sorted(evidence, key=lambda item: int(item.get("strength") or 0), reverse=True)[:8],
        "history": history,
        "source": "snapshot_fast" if data_basis == "行情快照" else "daily_kline_fast",
        "note": f"基于 {trade_date} 的{data_basis}生成主力快速分析，动量合成值 {momentum:+.1f}。",
    }


def _compute_mainforce_behavior(stock_code: str, trade_date: str = None) -> dict:
    """
    综合 K线形态 + 量价关系 + 资金流向 + 筹码变化 + 龙虎榜 五个维度，
    判断主力当前行为：建仓 / 洗盘 / 出货。
    返回每个信号维度的得分(0-100)和综合判断。
    得分越高越倾向"建仓"，越低越倾向"出货"。
    """
    if not trade_date:
        trade_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = str(trade_rows[0]["d"]) if trade_rows and trade_rows[0].get("d") else str(date.today())

    # ── 获取股票名称 ──
    name_rows = _read_sql("SELECT short_name FROM si_all_code WHERE stock_code = :c", {"c": stock_code})
    short_name = name_rows[0]["short_name"] if name_rows else stock_code

    # ── 1. 获取近60日K线（正序） ──
    kline_rows = _read_sql("""
        SELECT trade_date, open, close, high, low, volume, amount, change_pct, turnover_ratio
        FROM sm_stock_kline
        WHERE stock_code = :c AND k_type=1 AND adjust_type=0 AND trade_date <= :td
        ORDER BY trade_date DESC LIMIT 60
    """, {"c": stock_code, "td": trade_date})

    if not kline_rows or len(kline_rows) < 10:
        return {"stock_code": stock_code, "short_name": short_name, "behavior": "数据不足",
                "confidence": 0, "score": 50, "signals": {}, "history": []}

    # 转正序
    klines = list(reversed(kline_rows))
    n = len(klines)

    # 基础数组（null 保护：数据库中可能有 NULL 值）
    opens = [float(r["open"] or 0) for r in klines]
    closes = [float(r["close"] or 0) for r in klines]
    highs = [float(r["high"] or 0) for r in klines]
    lows = [float(r["low"] or 0) for r in klines]
    volumes = [float(r["volume"] or 0) for r in klines]
    changes = [float(r["change_pct"] or 0) for r in klines]
    turnover = [float(r["turnover_ratio"] or 0) for r in klines]

    # ── 2. 获取近20日资金流向（正序） ──
    flow_rows = _read_sql("""
        SELECT trade_date, main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow
        FROM sm_stock_capital_flow_daily
        WHERE stock_code = :c AND trade_date <= :td
        ORDER BY trade_date DESC LIMIT 20
    """, {"c": stock_code, "td": trade_date})
    flows = list(reversed(flow_rows)) if flow_rows else []
    flow_dates = [str(r["trade_date"]) for r in flows]
    main_flows = [float(r["main_net_inflow"] or 0) for r in flows]
    sm_flows = [float(r["sm_net_inflow"] or 0) for r in flows]

    # ── 3. 龙虎榜数据 ──
    lhb_rows = _read_sql("""
        SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy
        FROM st_a_list_daily
        WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 30 DAY)
    """, {"c": stock_code, "td": trade_date})
    lhb = lhb_rows[0] if lhb_rows else {}
    lhb_count = int(lhb.get("cnt") or 0)
    lhb_net_buy = float(lhb.get("inst_net_buy") or 0)

    # ── 4. 股东人数 ──
    holder_rows = _read_sql("""
        SELECT holder_num, holder_num_change, holder_num_ratio, avg_free_shares
        FROM si_stock_holder WHERE stock_code = :c
        ORDER BY report_date DESC LIMIT 2
    """, {"c": stock_code})
    holder = holder_rows[0] if holder_rows else {}
    holder_change = float(holder.get("holder_num_ratio") or 0) if holder else 0

    # ══════════════════════════════════════════════════════════════
    #  信号维度 1：量价背离信号 (权重 0.25)
    # ══════════════════════════════════════════════════════════════
    # 取近20日数据
    lookback = min(20, n)
    recent_closes = closes[-lookback:]
    recent_volumes = volumes[-lookback:]
    recent_turnover = turnover[-lookback:]

    # 价格变化率
    price_chg_pct = (recent_closes[-1] / recent_closes[0] - 1) * 100 if recent_closes[0] > 0 else 0
    # 成交量变化率
    vol_first_half = sum(recent_volumes[:lookback // 2]) / (lookback // 2) if lookback >= 4 else 1
    vol_second_half = sum(recent_volumes[lookback // 2:]) / (lookback - lookback // 2) if lookback >= 4 else 1
    vol_chg_pct = (vol_second_half / vol_first_half - 1) * 100 if vol_first_half > 0 else 0

    # 量价背离评分
    # 建仓：量增(20-80%) + 价平(-3%~+8%) → 高分
    # 出货：量增(>80%) + 价平/小跌 → 低分
    # 洗盘：量缩 + 价跌(-3%~-10%) → 中高分
    vp_score = 50
    vp_direction = "中性"
    vp_detail = ""

    if vol_chg_pct > 20 and -3 <= price_chg_pct <= 8:
        # 量增价平 → 建仓
        vp_score = 70 + min(15, (vol_chg_pct - 20) / 4)
        vp_direction = "建仓"
        vp_detail = f"近{lookback}日量能放大{vol_chg_pct:.0f}%，价格仅变动{price_chg_pct:+.1f}%，量价背离提示吸筹"
    elif vol_chg_pct > 80 and price_chg_pct <= 3:
        # 巨量价平/小跌 → 出货
        vp_score = 25
        vp_direction = "出货"
        vp_detail = f"近{lookback}日量能暴增{vol_chg_pct:.0f}%但价格仅{price_chg_pct:+.1f}%，高位放量滞涨"
    elif vol_chg_pct < -20 and -10 <= price_chg_pct <= 0:
        # 缩量回调 → 洗盘
        vp_score = 72
        vp_direction = "洗盘"
        vp_detail = f"近{lookback}日量能萎缩{abs(vol_chg_pct):.0f}%，价格小幅回调{price_chg_pct:+.1f}%，缩量洗盘"
    elif vol_chg_pct > 50 and price_chg_pct > 8:
        # 量价齐升但量增过多 → 出货风险
        vp_score = 38
        vp_direction = "出货"
        vp_detail = f"近{lookback}日量价齐升但量能暴增{vol_chg_pct:.0f}%，需警惕拉高出货"
    elif 5 <= vol_chg_pct <= 30 and price_chg_pct > 0:
        # 温和放量上涨 → 偏建仓
        vp_score = 65
        vp_direction = "建仓"
        vp_detail = f"近{lookback}日温和放量{vol_chg_pct:.0f}%，价格上涨{price_chg_pct:+.1f}%，健康上行"
    else:
        vp_detail = f"近{lookback}日量变{vol_chg_pct:+.0f}%，价变{price_chg_pct:+.1f}%，无明显信号"

    # ══════════════════════════════════════════════════════════════
    #  信号维度 2：资金流向信号 (权重 0.30)
    # ══════════════════════════════════════════════════════════════
    cf_score = 50
    cf_direction = "中性"
    cf_detail = ""

    if len(flows) >= 5:
        flow_5d = sum(main_flows[-5:])
        flow_10d = sum(main_flows[-10:]) if len(flows) >= 10 else sum(main_flows)
        flow_20d = sum(main_flows)

        # 散户流向（近5日）
        sm_5d = sum(sm_flows[-5:]) if sm_flows else 0

        # 连续流入天数（近10日）
        consecutive_in = 0
        for f in main_flows[-10:][::-1]:
            if f > 0:
                consecutive_in += 1
            else:
                break

        # 连续流出天数
        consecutive_out = 0
        for f in main_flows[-10:][::-1]:
            if f < 0:
                consecutive_out += 1
            else:
                break

        # 评分逻辑
        if flow_5d > 0 and flow_20d > 0 and price_chg_pct < 10:
            # 持续净流入 + 涨幅不大 → 建仓
            cf_score = 65 + min(20, consecutive_in * 3)
            cf_direction = "建仓"
            cf_detail = f"5日主力净流入{flow_5d/1e8:.2f}亿，20日净流入{flow_20d/1e8:.2f}亿，连续{consecutive_in}日净流入"
        elif flow_5d < 0 and flow_20d < 0 and sm_5d > 0:
            # 主力流出 + 散户流入 → 出货
            cf_score = 22
            cf_direction = "出货"
            cf_detail = f"5日主力净流出{abs(flow_5d)/1e8:.2f}亿，散户净流入{sm_5d/1e8:.2f}亿，筹码转移"
        elif flow_5d < 0 and flow_20d > 0:
            # 短期流出 + 中期流入 → 洗盘
            cf_score = 62
            cf_direction = "洗盘"
            cf_detail = f"5日主力小幅净流出{abs(flow_5d)/1e8:.2f}亿，但20日累计净流入{flow_20d/1e8:.2f}亿，短期洗盘"
        elif flow_5d > 0 and flow_20d < 0:
            # 短期流入 + 中期流出 → 可能反弹出货
            cf_score = 40
            cf_direction = "出货"
            cf_detail = f"5日主力净流入{flow_5d/1e8:.2f}亿，但20日累计净流出{abs(flow_20d)/1e8:.2f}亿，反弹出货可能"
        elif consecutive_out >= 5:
            cf_score = 20
            cf_direction = "出货"
            cf_detail = f"主力连续{consecutive_out}日净流出，持续出货"
        elif consecutive_in >= 5:
            cf_score = 80
            cf_direction = "建仓"
            cf_detail = f"主力连续{consecutive_in}日净流入，持续吸筹"
        else:
            cf_detail = f"5日主力净流入{flow_5d/1e8:.2f}亿，资金方向不明确"
    else:
        cf_detail = "资金流向数据不足"

    # ══════════════════════════════════════════════════════════════
    #  信号维度 3：K线形态信号 (权重 0.20)
    # ══════════════════════════════════════════════════════════════
    kl_score = 50
    kl_direction = "中性"
    kl_detail = ""

    # 近20日K线分析
    kl_lookback = min(20, n)
    kl_opens = opens[-kl_lookback:]
    kl_closes = closes[-kl_lookback:]
    kl_highs = highs[-kl_lookback:]
    kl_lows = lows[-kl_lookback:]

    # 振幅分析（建仓期振幅小）
    amplitudes = [(h - l) / c * 100 for h, l, c in zip(kl_highs, kl_lows, kl_closes) if c > 0]
    avg_amplitude = sum(amplitudes) / len(amplitudes) if amplitudes else 5

    # 阴阳线统计
    yang_count = sum(1 for o, c in zip(kl_opens, kl_closes) if c > o)  # 阳线
    yin_count = sum(1 for o, c in zip(kl_opens, kl_closes) if c < o)   # 阴线

    # 上下影线分析
    upper_shadows = []
    lower_shadows = []
    for o, c, h, l in zip(kl_opens, kl_closes, kl_highs, kl_lows):
        body_top = max(o, c)
        body_bot = min(o, c)
        if h > body_top:
            upper_shadows.append((h - body_top) / (h - l) * 100 if h != l else 0)
        if l < body_bot:
            lower_shadows.append((body_bot - l) / (h - l) * 100 if h != l else 0)
    avg_upper = sum(upper_shadows) / len(upper_shadows) if upper_shadows else 0
    avg_lower = sum(lower_shadows) / len(lower_shadows) if lower_shadows else 0

    # 当前价格在近20日区间的位置
    range_high = max(kl_highs)
    range_low = min(kl_lows)
    range_pos = (kl_closes[-1] - range_low) / (range_high - range_low) * 100 if range_high != range_low else 50

    # 评分
    if avg_amplitude < 3.5 and 35 <= range_pos <= 65:
        # 低振幅 + 中间位置 → 建仓横盘
        kl_score = 75
        kl_direction = "建仓"
        kl_detail = f"近{kl_lookback}日平均振幅{avg_amplitude:.1f}%，价格处于区间中部{range_pos:.0f}%，低位横盘蓄势"
    elif avg_amplitude < 4 and yang_count > yin_count and range_pos > 60:
        # 低振幅偏阳 + 位置偏高 → 偏建仓
        kl_score = 68
        kl_direction = "建仓"
        kl_detail = f"近{kl_lookback}日振幅{avg_amplitude:.1f}%，阳线{yang_count}根多于阴线{yin_count}根，温和上行"
    elif avg_lower > avg_upper * 1.5 and price_chg_pct < -3:
        # 长下影 + 价格回调 → 洗盘
        kl_score = 70
        kl_direction = "洗盘"
        kl_detail = f"近{kl_lookback}日下影线比例{avg_lower:.0f}%显著高于上影线{avg_upper:.0f}%，主力托底洗盘"
    elif avg_upper > avg_lower * 1.8 and range_pos > 70:
        # 长上影 + 高位 → 出货
        kl_score = 28
        kl_direction = "出货"
        kl_detail = f"近{kl_lookback}日上影线比例{avg_upper:.0f}%高于下影线{avg_lower:.0f}%，高位抛压明显"
    elif avg_amplitude > 6 and range_pos > 75:
        # 高振幅 + 高位 → 出货
        kl_score = 30
        kl_direction = "出货"
        kl_detail = f"近{kl_lookback}日平均振幅{avg_amplitude:.1f}%较大，价格处于高位{range_pos:.0f}%，主力活跃出货"
    elif range_pos < 30 and avg_amplitude < 5:
        # 低位 + 低振幅 → 建仓
        kl_score = 73
        kl_direction = "建仓"
        kl_detail = f"价格处于区间低位{range_pos:.0f}%，振幅{avg_amplitude:.1f}%偏小，底部建仓可能"
    else:
        kl_detail = f"近{kl_lookback}日振幅{avg_amplitude:.1f}%，阴阳比{yang_count}:{yin_count}，区间位置{range_pos:.0f}%"

    # ══════════════════════════════════════════════════════════════
    #  信号维度 4：筹码集中信号 (权重 0.15)
    # ══════════════════════════════════════════════════════════════
    chip_score = 50
    chip_direction = "中性"
    chip_detail = ""

    # 换手率趋势
    tr_first = sum(recent_turnover[:lookback // 2]) / (lookback // 2) if lookback >= 4 else 3
    tr_second = sum(recent_turnover[lookback // 2:]) / (lookback - lookback // 2) if lookback >= 4 else 3
    tr_trend = tr_second - tr_first  # 正=换手率上升

    # 股东人数变化
    if holder_change < -5:
        # 股东人数大幅减少 → 筹码集中 → 建仓
        chip_score = 80
        chip_direction = "建仓"
        chip_detail = f"股东人数减少{abs(holder_change):.1f}%，筹码显著集中"
    elif holder_change < -2:
        chip_score = 68
        chip_direction = "建仓"
        chip_detail = f"股东人数减少{abs(holder_change):.1f}%，筹码趋于集中"
    elif holder_change > 5:
        # 股东人数大幅增加 → 筹码分散 → 出货
        chip_score = 25
        chip_direction = "出货"
        chip_detail = f"股东人数增加{holder_change:.1f}%，筹码分散"
    elif holder_change > 2:
        chip_score = 38
        chip_direction = "出货"
        chip_detail = f"股东人数增加{holder_change:.1f}%，筹码趋于分散"
    else:
        # 无股东数据，用换手率趋势辅助判断
        if 3 <= tr_second <= 8 and tr_trend > 0:
            chip_score = 65
            chip_direction = "建仓"
            chip_detail = f"换手率从{tr_first:.1f}%升至{tr_second:.1f}%，温和递增，资金有序介入"
        elif tr_second > 12 and price_chg_pct > 5:
            chip_score = 32
            chip_direction = "出货"
            chip_detail = f"换手率高达{tr_second:.1f}%，高位换手频繁"
        elif tr_second < 3 and price_chg_pct < -3:
            chip_score = 65
            chip_direction = "洗盘"
            chip_detail = f"换手率降至{tr_second:.1f}%，回调中抛压减轻"
        else:
            chip_detail = f"换手率趋势{tr_first:.1f}%→{tr_second:.1f}%，股东数据暂缺"

    # ══════════════════════════════════════════════════════════════
    #  信号维度 5：龙虎榜/机构信号 (权重 0.10)
    # ══════════════════════════════════════════════════════════════
    inst_score = 50
    inst_direction = "中性"
    inst_detail = ""

    if lhb_count > 0:
        if lhb_net_buy > 0:
            inst_score = 70 + min(15, lhb_net_buy / 1e8 * 5)
            inst_direction = "建仓"
            inst_detail = f"近30日{lhb_count}次龙虎榜，机构净买入{lhb_net_buy/1e8:.2f}亿"
        elif lhb_net_buy < 0:
            inst_score = 25
            inst_direction = "出货"
            inst_detail = f"近30日{lhb_count}次龙虎榜，机构净卖出{abs(lhb_net_buy)/1e8:.2f}亿"
        else:
            inst_detail = f"近30日{lhb_count}次龙虎榜，买卖平衡"
    else:
        inst_detail = "近30日无龙虎榜记录"

    # ══════════════════════════════════════════════════════════════
    #  综合评分
    # ══════════════════════════════════════════════════════════════
    weights = {"volume_price": 0.25, "capital_flow": 0.30, "kline_pattern": 0.20,
               "chip_concentration": 0.15, "institutional": 0.10}
    scores = {
        "volume_price": vp_score,
        "capital_flow": cf_score,
        "kline_pattern": kl_score,
        "chip_concentration": chip_score,
        "institutional": inst_score,
    }
    total_score = sum(scores[k] * weights[k] for k in weights)
    total_score = round(total_score, 1)

    # 综合行为判断
    # 收集各维度方向的投票
    direction_votes = {"建仓": 0, "洗盘": 0, "出货": 0, "中性": 0}
    for name, direction in [("volume_price", vp_direction), ("capital_flow", cf_direction),
                             ("kline_pattern", kl_direction), ("chip_concentration", chip_direction),
                             ("institutional", inst_direction)]:
        direction_votes[direction] += weights[name]

    # 根据综合得分和方向投票判断
    if total_score >= 62:
        behavior = "建仓"
    elif total_score <= 38:
        behavior = "出货"
    elif total_score >= 55:
        # 偏建仓但不够强，看投票
        if direction_votes["洗盘"] > direction_votes["出货"]:
            behavior = "洗盘"
        else:
            behavior = "建仓"
    elif total_score <= 45:
        if direction_votes["洗盘"] > direction_votes["建仓"]:
            behavior = "洗盘"
        else:
            behavior = "出货"
    else:
        # 中间区域，看哪个方向票多
        if direction_votes["洗盘"] >= direction_votes["建仓"] and direction_votes["洗盘"] >= direction_votes["出货"]:
            behavior = "洗盘"
        elif direction_votes["建仓"] > direction_votes["出货"]:
            behavior = "建仓"
        else:
            behavior = "出货"

    # 置信度：各维度方向一致性
    max_vote = max(direction_votes["建仓"], direction_votes["洗盘"], direction_votes["出货"])
    confidence = round(min(95, max_vote * 100 + 20))

    # ── 构建逐日得分历史 ──
    history = []
    for i in range(max(0, n - 20), n):
        # 简化：用当日量价和资金流向计算每日得分
        d = str(klines[i]["trade_date"])
        day_c = closes[i]
        day_v = volumes[i]
        day_chg = changes[i]
        day_tr = turnover[i]

        # 当日量价简单评分
        day_vp = 50
        if day_chg > 0 and day_v > (sum(volumes[max(0, i-5):i]) / max(1, min(5, i)) * 1.2):
            day_vp = 65  # 放量上涨
        elif day_chg < 0 and day_v < (sum(volumes[max(0, i-5):i]) / max(1, min(5, i)) * 0.8):
            day_vp = 62  # 缩量下跌（洗盘）
        elif day_chg > 3 and day_v > (sum(volumes[max(0, i-5):i]) / max(1, min(5, i)) * 1.8):
            day_vp = 35  # 暴量上涨（出货风险）

        # 当日资金流向
        day_cf = 50
        if d in flow_dates:
            idx = flow_dates.index(d)
            if main_flows[idx] > 0:
                day_cf = 65
            elif main_flows[idx] < 0:
                day_cf = 35

        day_score = round(day_vp * 0.5 + day_cf * 0.5, 1)
        day_behavior = "建仓" if day_score >= 60 else "出货" if day_score <= 40 else "洗盘"
        history.append({"date": d, "score": day_score, "behavior": day_behavior})

    return {
        "stock_code": stock_code,
        "short_name": short_name,
        "behavior": behavior,
        "confidence": confidence,
        "score": total_score,
        "signals": {
            "volume_price": {"score": round(vp_score), "direction": vp_direction, "detail": vp_detail},
            "capital_flow": {"score": round(cf_score), "direction": cf_direction, "detail": cf_detail},
            "kline_pattern": {"score": round(kl_score), "direction": kl_direction, "detail": kl_detail},
            "chip_concentration": {"score": round(chip_score), "direction": chip_direction, "detail": chip_detail},
            "institutional": {"score": round(inst_score), "direction": inst_direction, "detail": inst_detail},
        },
        "history": history,
    }


@router.get("/hot-data/mainforce-analysis")
def mainforce_analysis(stock_code: str = Query(...), trade_date: str = Query(default=None)):
    """单只股票主力行为分析"""
    trade_date = trade_date if isinstance(trade_date, str) else ""
    if not trade_date:
        trade_date = _latest_market_analysis_date()

    cache_key = f"mainforce_analysis_{stock_code}_{trade_date}"
    cached = _cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    result = _compute_mainforce_behavior_fast(stock_code, trade_date)
    _cache_set(cache_key, result)
    return result


@router.get("/hot-data/mainforce-scan")
def mainforce_scan(trade_date: str = Query(default=None), top: int = Query(default=50, ge=1, le=200)):
    """
    全市场主力行为扫描。
    先从 sm_stock_snapshot 获取活跃股，批量计算主力行为，返回得分排名。
    """
    # 缓存 120 秒（扫描计算量大）
    _ckey = f"mainforce_scan_{trade_date}_{top}"
    cached = _cache_get(_ckey, ttl_seconds=120)
    if cached is not None:
        return cached
    trade_date = trade_date if isinstance(trade_date, str) else ""
    if not trade_date:
        trade_date = _latest_date("sm_stock_snapshot")

    # 取有资金流向数据的活跃股（排除ST，排除北交所）
    candidates = _read_sql(f"""
        SELECT s.stock_code, s.short_name, s.change_pct, s.price, s.main_net_inflow,
               s.turnover_ratio, s.amount
        FROM sm_stock_snapshot s
        WHERE s.trade_date = :td
          AND s.short_name NOT LIKE '%%ST%%'
          AND s.stock_code NOT LIKE '8%%'
          AND s.stock_code NOT LIKE '4%%'
          AND s.amount > 50000000
        ORDER BY ABS(s.main_net_inflow) DESC
        LIMIT :lim
    """, {"td": trade_date, "lim": top * 3})

    if not candidates:
        return {"trade_date": trade_date, "results": [], "summary": {"建仓": 0, "洗盘": 0, "出货": 0}}

    # 并行分析各股票主力行为（原来串行逐个查库，并行后快 4-8 倍）
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _analyze_one(row):
        code = row["stock_code"]
        try:
            analysis = _compute_mainforce_behavior_fast(code, trade_date)
            if analysis.get("behavior") in ("建仓", "洗盘", "出货"):
                return {
                    "stock_code": code,
                    "short_name": row.get("short_name") or analysis.get("short_name"),
                    "price": row.get("price"),
                    "change_pct": row.get("change_pct"),
                    "amount": row.get("amount"),
                    "main_net_inflow": row.get("main_net_inflow"),
                    "behavior": analysis["behavior"],
                    "confidence": analysis["confidence"],
                    "score": analysis["score"],
                    "signals": {k: {"score": v["score"], "direction": v["direction"]}
                                for k, v in analysis.get("signals", {}).items()},
                }
        except Exception as exc:
            _record_fallback('_analyze_one:9411', exc)
        return None

    results = []
    summary = {"建仓": 0, "洗盘": 0, "出货": 0}

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_analyze_one, row): row for row in candidates}
        for future in as_completed(futures):
            item = future.result()
            if item:
                results.append(item)
                summary[item["behavior"]] = summary.get(item["behavior"], 0) + 1

    # 按置信度降序排列，取前 top 个
    results.sort(key=lambda x: (-x["confidence"], -x["score"]))
    results = results[:top]

    _result = {"trade_date": trade_date, "results": results, "summary": summary}
    _cache_set(_ckey, _result)
    return _result


# ──────────────────────────────────────────────────────────────────────
#  板块轮动分析（调仓换股决策辅助）
# ──────────────────────────────────────────────────────────────────────

def _sector_rotation_signal(
    rising_sectors: list[dict],
    falling_sectors: list[dict],
    flow_in_top: list[dict],
    flow_out_top: list[dict],
    *,
    flow_snapshot_at: str | None = None,
) -> dict:
    rising = [s for s in (rising_sectors or []) if float(s.get("momentum") or 0) > 3]
    falling = [s for s in (falling_sectors or []) if float(s.get("momentum") or 0) < -3]
    flow_in = [f for f in (flow_in_top or []) if float(f.get("main_net_inflow") or 0) > 0]
    flow_out = [f for f in (flow_out_top or []) if float(f.get("main_net_inflow") or 0) < 0]

    rising_names = {str(s.get("name") or "") for s in rising}
    falling_names = {str(s.get("name") or "") for s in falling}
    to_candidates = [dict(s, flow_match=any(str(f.get("name") or "") == str(s.get("name") or "") for f in flow_in)) for s in rising[:5]]
    from_candidates = [dict(s, flow_match=any(str(f.get("name") or "") == str(s.get("name") or "") for f in flow_out)) for s in falling[:5]]

    fund_in_names = [str(f.get("name") or "") for f in flow_in[:5]]
    fund_out_names = [str(f.get("name") or "") for f in flow_out[:5]]
    aligned_in = [name for name in fund_in_names if name in rising_names]
    aligned_out = [name for name in fund_out_names if name in falling_names]

    if to_candidates and from_candidates and (flow_in or flow_out):
        status = "switching"
        risk_level = "medium"
        summary = f"板块切换中：{from_candidates[0]['name']}等退潮，{to_candidates[0]['name']}等走强"
    elif to_candidates and flow_in:
        status = "inflow"
        risk_level = "low"
        summary = f"资金偏进攻：{to_candidates[0]['name']}等走强，观察能否持续放量"
    elif from_candidates and flow_out:
        status = "outflow"
        risk_level = "high"
        summary = f"资金偏防守：{from_candidates[0]['name']}等退潮，优先控制回撤"
    else:
        status = "balanced"
        risk_level = "low"
        summary = "板块轮动不明显，暂未形成清晰切换方向"

    action = {
        "switching": "从退潮板块降仓，优先观察资金流入且动量上升的板块",
        "inflow": "可跟踪流入板块中的龙头强弱，避免追高一次性满仓",
        "outflow": "优先控制回撤，降低高波动仓位，等资金重新回流后再加仓",
        "balanced": "维持现有仓位，等待主线确认",
    }[status]

    return {
        "status": status,
        "summary": summary,
        "action": action,
        "risk_level": risk_level,
        "to_sectors": to_candidates,
        "from_sectors": from_candidates,
        "fund_in_sectors": flow_in[:5],
        "fund_out_sectors": flow_out[:5],
        "aligned_in": aligned_in,
        "aligned_out": aligned_out,
        "flow_snapshot_at": flow_snapshot_at or "",
    }


@router.get("/hot-data/sector-rotation")
def sector_rotation(trade_date: str = Query(default=None), days: int = Query(default=10, ge=3, le=30)):
    """
    板块轮动分析：告诉你哪些板块在退潮、哪些在崛起，给出调仓换股建议。
    综合三个维度：热度排名变化 + 板块K线动量 + 板块资金流向。
    """
    if not trade_date:
        td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = str(td_rows[0]["d"]) if td_rows and td_rows[0].get("d") else str(date.today())

    _ttl = _market_live_cache_ttl()
    _ckey = f"sector_rotation_{trade_date}_{days}"
    cached = _cache_get(_ckey, ttl_seconds=_ttl)
    if cached is not None:
        return cached

    # ── 1. 获取近 N 天的行业热度排名数据 ──
    # 优先使用东财数据（plate_type=3行业/4二级行业，真实成交额），回退到THS数据（plate_type=1概念/2行业，搜索热度）
    from datetime import timedelta
    td = datetime.strptime(trade_date, "%Y-%m-%d")
    start_date = (td - timedelta(days=days * 2)).strftime("%Y-%m-%d")  # 多取一些天

    # 先查东财行业数据（plate_type=3）是否有足够天数
    east_dates = _read_sql("""
        SELECT DISTINCT snapshot_date FROM st_hot_concept_ths_daily
        WHERE snapshot_date >= :sd AND snapshot_date <= :td AND plate_type = 3
        ORDER BY snapshot_date
    """, {"sd": start_date, "td": trade_date})

    use_east = len(east_dates) >= max(days // 2, 5)  # 东财数据至少需要足够天数才用

    if use_east:
        # 用东财数据：行业用 plate_type=3，概念仍用 plate_type=1
        rank_rows = _read_sql("""
            SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name, change_pct, hot_value
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date >= :sd AND snapshot_date <= :td
              AND (plate_type = 3 OR plate_type = 1)
            ORDER BY snapshot_date, plate_type, `rank`
        """, {"sd": start_date, "td": trade_date})
        data_source = "east"
    else:
        # 回退到THS数据
        rank_rows = _read_sql("""
            SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name, change_pct, hot_value
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date >= :sd AND snapshot_date <= :td AND plate_type IN (1, 2)
            ORDER BY snapshot_date, plate_type, `rank`
        """, {"sd": start_date, "td": trade_date})
        data_source = "ths"

    if not rank_rows:
        result = {"trade_date": trade_date, "error": "暂无板块排名数据"}
        _cache_set(_ckey, result)
        return result

    # 分离行业和概念数据
    industry_data = {}  # {snapshot_date: [{rank, name, change_pct, hot_value}]}
    concept_data = {}
    for r in rank_rows:
        d = str(r["snapshot_date"])
        item = {"rank": r["rank"], "code": r.get("concept_code"), "name": r["concept_name"],
                "change_pct": float(r["change_pct"] or 0), "hot_value": float(r["hot_value"] or 0)}
        pt = r["plate_type"]
        if pt in (2, 3):  # 行业数据：THS(2) 或 东财一级(3)
            industry_data.setdefault(d, []).append(item)
        else:  # 概念数据：THS(1) 或 东财二级(4)
            concept_data.setdefault(d, []).append(item)

    # 排序得到最近的日期列表
    all_dates = sorted(set(str(r["snapshot_date"]) for r in rank_rows))
    recent_dates = all_dates[-days:] if len(all_dates) >= days else all_dates

    if len(recent_dates) < 2:
        result = {"trade_date": trade_date, "error": "数据天数不足"}
        _cache_set(_ckey, result)
        return result

    # ── 2. 分析行业排名变化趋势 ──
    # 对每个行业，计算近 N 天的排名趋势
    def analyze_rank_trends(data_by_date, dates, top_n=15):
        """分析板块排名变化，返回：rising(崛起), falling(退潮), stable(稳定)"""
        sector_trends = {}  # {name: {ranks: [], changes: [], hot_values: [], dates: []}}

        for d in dates:
            items = data_by_date.get(d, [])
            for item in items[:top_n]:  # 只看 TOP N
                name = item["name"]
                if name not in sector_trends:
                    sector_trends[name] = {"ranks": [], "changes": [], "hot_values": [], "dates": []}
                sector_trends[name]["ranks"].append(item["rank"])
                sector_trends[name]["changes"].append(item["change_pct"])
                sector_trends[name]["hot_values"].append(item["hot_value"])
                sector_trends[name]["dates"].append(d)

        results = []
        for name, trend in sector_trends.items():
            if len(trend["ranks"]) < 2:
                continue
            # 排名趋势：最近排名 vs 之前排名（排名数字越小越好）
            recent_ranks = trend["ranks"][-3:] if len(trend["ranks"]) >= 3 else trend["ranks"]
            early_ranks = trend["ranks"][:3] if len(trend["ranks"]) >= 3 else trend["ranks"][:1]
            avg_recent_rank = sum(recent_ranks) / len(recent_ranks)
            avg_early_rank = sum(early_ranks) / len(early_ranks)
            rank_change = avg_early_rank - avg_recent_rank  # 正数=排名上升(好)

            # 涨跌幅趋势
            recent_changes = trend["changes"][-3:] if len(trend["changes"]) >= 3 else trend["changes"]
            early_changes = trend["changes"][:3] if len(trend["changes"]) >= 3 else trend["changes"][:1]
            avg_recent_chg = sum(recent_changes) / len(recent_changes)
            avg_early_chg = sum(early_changes) / len(early_changes)
            chg_trend = avg_recent_chg - avg_early_chg  # 正数=涨势增强

            # 热度趋势（用log压缩消除不同数据源量纲差异）
            import math as _math
            recent_hot = trend["hot_values"][-3:] if len(trend["hot_values"]) >= 3 else trend["hot_values"]
            early_hot = trend["hot_values"][:3] if len(trend["hot_values"]) >= 3 else trend["hot_values"][:1]
            avg_recent_hot = sum(recent_hot) / len(recent_hot)
            avg_early_hot = sum(early_hot) / len(early_hot)
            # log压缩后计算变化率，消除THS(0-100)和东财(成交额)的量纲差异
            log_recent = _math.log(max(avg_recent_hot, 0) + 1)
            log_early = _math.log(max(avg_early_hot, 0) + 1)
            hot_trend = log_recent - log_early  # 正数=热度上升

            # 综合动量得分（三项均为正值=向好）
            momentum = rank_change * 3 + chg_trend * 2 + hot_trend * 15

            results.append({
                "name": name,
                "avg_rank": round(avg_recent_rank, 1),
                "rank_change": round(rank_change, 1),
                "avg_change_pct": round(avg_recent_chg, 2),
                "chg_trend": round(chg_trend, 2),
                "hot_trend": round(hot_trend, 1),
                "momentum": round(momentum, 1),
                "recent_ranks": trend["ranks"][-5:],
                "recent_changes": [round(c, 2) for c in trend["changes"][-5:]],
                "appear_days": len(trend["ranks"]),
            })

        results.sort(key=lambda x: -x["momentum"])
        return results

    industry_trends = analyze_rank_trends(industry_data, recent_dates, top_n=15)
    concept_trends = analyze_rank_trends(concept_data, recent_dates, top_n=15)

    # ── 3. 获取板块资金流向（东财板块资金流） ──
    # 取最近的快照
    flow_snap_rows = _read_sql("""
        SELECT MAX(snapshot_at) AS snap FROM sm_concept_capital_flow_east WHERE days_type = 1
    """)
    flow_snap = flow_snap_rows[0]["snap"] if flow_snap_rows and flow_snap_rows[0].get("snap") else None

    sector_flows = {}  # {index_name: {main_net_inflow, main_net_inflow_rate, ...}}
    if flow_snap:
        flow_rows = _read_sql("""
            SELECT index_name, main_net_inflow, main_net_inflow_rate,
                   max_net_inflow, lg_net_inflow, stock_name
            FROM sm_concept_capital_flow_east
            WHERE days_type = 1 AND snapshot_at = :snap
            ORDER BY main_net_inflow DESC
        """, {"snap": flow_snap})
        for r in flow_rows:
            sector_flows[r["index_name"]] = {
                "main_net_inflow": float(r["main_net_inflow"] or 0),
                "main_net_inflow_rate": float(r["main_net_inflow_rate"] or 0),
                "max_net_inflow": float(r["max_net_inflow"] or 0),
                "lg_net_inflow": float(r["lg_net_inflow"] or 0),
                "leader_stock": r.get("stock_name", ""),
            }

    # ── 4. 宏观板块分组汇总 ──
    # 把行业映射到宏观板块组
    def map_to_group(name):
        """将行业/概念名称映射到宏观板块组"""
        # 先精确匹配
        if name in INDUSTRY_TO_GROUP:
            return INDUSTRY_TO_GROUP[name]
        # 再通过 INDUSTRY_NAME_MAP 转换
        mapped = INDUSTRY_NAME_MAP.get(name)
        if mapped and mapped in INDUSTRY_TO_GROUP:
            return INDUSTRY_TO_GROUP[mapped]
        # 模糊匹配
        for industry, group in INDUSTRY_TO_GROUP.items():
            if industry in name or name in industry:
                return group
        return "其他"

    group_stats = {}  # {group_name: {momentum_sum, count, rising: [], falling: [], ...}}
    for trend in industry_trends:
        group = map_to_group(trend["name"])
        if group not in group_stats:
            group_stats[group] = {"momentum_sum": 0, "count": 0, "rising": [], "falling": [], "sectors": []}
        gs = group_stats[group]
        gs["momentum_sum"] += trend["momentum"]
        gs["count"] += 1
        gs["sectors"].append(trend["name"])
        if trend["momentum"] > 5:
            gs["rising"].append(trend["name"])
        elif trend["momentum"] < -5:
            gs["falling"].append(trend["name"])

    # 计算每组平均动量
    group_momentum = []
    for gname, gs in group_stats.items():
        avg_mom = gs["momentum_sum"] / gs["count"] if gs["count"] > 0 else 0
        group_momentum.append({
            "group": gname,
            "avg_momentum": round(avg_mom, 1),
            "rising_sectors": gs["rising"],
            "falling_sectors": gs["falling"],
            "sector_count": gs["count"],
        })
    group_momentum.sort(key=lambda x: -x["avg_momentum"])

    # ── 5. 生成调仓建议 ──
    rising_groups = [g for g in group_momentum if g["avg_momentum"] > 3]
    falling_groups = [g for g in group_momentum if g["avg_momentum"] < -3]

    advice = []

    # 退潮板块
    if falling_groups:
        for g in falling_groups:
            sectors_str = "、".join(g["falling_sectors"][:3]) if g["falling_sectors"] else "、".join(g["rising_sectors"][:2])
            advice.append({
                "type": "reduce",
                "group": g["group"],
                "text": f"{g['group']}板块整体走弱，{sectors_str}等在回调，建议减仓或观望",
                "momentum": g["avg_momentum"],
                "sectors": g["falling_sectors"] or g["rising_sectors"],
            })

    # 崛起板块
    if rising_groups:
        for g in rising_groups:
            sectors_str = "、".join(g["rising_sectors"][:3]) if g["rising_sectors"] else ""
            advice.append({
                "type": "add",
                "group": g["group"],
                "text": f"{g['group']}板块正在崛起，{sectors_str}等领涨，可考虑加仓布局",
                "momentum": g["avg_momentum"],
                "sectors": g["rising_sectors"],
            })

    # 如果没有明确方向
    if not rising_groups and not falling_groups:
        advice.append({
            "type": "hold",
            "group": "全市场",
            "text": "各板块动量差异不大，市场无明确主线，建议持仓观望",
            "momentum": 0,
            "sectors": [],
        })

    # ── 6. 崛起/退潮行业 TOP 排行 ──
    rising_sectors = [t for t in industry_trends if t["momentum"] > 3][:10]
    falling_sectors = [t for t in industry_trends if t["momentum"] < -3]
    falling_sectors.sort(key=lambda x: x["momentum"])
    falling_sectors = falling_sectors[:10]

    # 概念板块崛起 TOP
    rising_concepts = [t for t in concept_trends if t["momentum"] > 3][:10]

    # ── 7. 资金流入/流出 TOP 行业 ──
    flow_sorted = sorted(sector_flows.items(), key=lambda x: -x[1]["main_net_inflow"])
    flow_in_top = [{"name": k, **v} for k, v in flow_sorted[:10]]
    flow_out_top = [{"name": k, **v} for k, v in flow_sorted[-10:]]
    flow_out_top.reverse()
    rotation_signal = _sector_rotation_signal(
        rising_sectors,
        falling_sectors,
        flow_in_top,
        flow_out_top,
        flow_snapshot_at=str(flow_snap or "")[:19],
    )

    result = {
        "trade_date": trade_date,
        "data_source": data_source,
        "flow_snapshot_at": str(flow_snap or "")[:19],
        "rotation_signal": rotation_signal,
        "lookback_days": len(recent_dates),
        "group_momentum": group_momentum,
        "advice": advice,
        "rising_sectors": rising_sectors,
        "falling_sectors": falling_sectors,
        "rising_concepts": rising_concepts,
        "flow_in_top": flow_in_top,
        "flow_out_top": flow_out_top,
        "industry_trends": industry_trends[:20],
        "concept_trends": concept_trends[:20],
    }
    _cache_set(_ckey, result)
    return result


# ── 盘中实时数据刷新 ──
@router.post("/realtime/refresh")
def realtime_refresh(only: str = Query(default="all", regex="^(all|snapshot|flow|concept|index)$")):
    """手动刷新盘中数据（行情快照、资金流向、概念行情、指数行情）"""
    import subprocess as _sp
    if not _job_begin("market_refresh"):
        return {"success": False, "busy": True, "error": "market_refresh_running"}
    script = str(_ROOT / "tools" / "crawl_realtime_batch.py")
    cmd = [
        sys.executable,
        script,
        "--only",
        only,
        "--json",
        "--min-coverage",
        "0.70",
    ]
    if only in ("snapshot", "all"):
        cmd.append("--archive-snapshot")
    if only in ("snapshot", "all", "concept", "index"):
        cmd.append("--skip-closed")
    env = build_child_env(_ROOT, engine=get_engine())
    try:
        result = _sp.run(
            cmd,
            capture_output=True, text=True, timeout=120, env=env,
            cwd=str(_ROOT),
        )
        if result.returncode == 0:
            _invalidate_market_runtime_caches()
        return {
            "success": result.returncode == 0,
            "output": result.stdout[-500:] if result.stdout else "",
            "error": result.stderr[-500:] if result.stderr else "",
        }
    except _sp.TimeoutExpired:
        return {"success": False, "error": "timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        _job_end("market_refresh")
