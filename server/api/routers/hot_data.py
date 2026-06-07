# -*- coding: utf-8 -*-
"""热门数据查询 API + 首页看板"""
import json
import os
import re
from datetime import date, datetime

import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import text

from server.api.routers._engine import get_engine
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
except Exception:
    _CHARTS_AVAILABLE = False

router = APIRouter()

LIVE_FUSED_SOURCE_LABEL = "东财人气榜 / 雪球热股 / 新浪热股 / 同花顺热股"


# ── 内存 TTL 缓存（避免重复请求外部 API / 历史数据） ──
import threading as _threading

_cache_lock = _threading.Lock()
_cache_store: dict[str, tuple[float, object]] = {}


def _cache_get(key: str, ttl_seconds: int = 60):
    """获取缓存，未过期返回值，否则返回 None"""
    with _cache_lock:
        entry = _cache_store.get(key)
        if entry and (datetime.now().timestamp() - entry[0]) < ttl_seconds:
            return entry[1]
    return None


def _cache_set(key: str, value):
    """写入缓存"""
    with _cache_lock:
        _cache_store[key] = (datetime.now().timestamp(), value)


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    import numpy as np
    df = pd.read_sql(text(sql), get_engine(), params=params)
    if df.empty:
        return []
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    for c in df.columns:
        if df[c].dtype == "datetime64[ns]":
            df[c] = df[c].astype(str)
    return df.to_dict(orient="records")


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    import numpy as np

    if df is None or df.empty:
        return []
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    for c in df.columns:
        if str(df[c].dtype).startswith("datetime64"):
            df[c] = df[c].astype(str)
    return df.to_dict(orient="records")


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
    except Exception:
        pass

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
                except Exception:
                    pass
        for col in ("price", "price_change", "change_pct"):
            df[col] = df["stock_code"].map(lambda code: quote_map.get(code, {}).get(col))
    except Exception:
        pass
    return df


def _fetch_live_ths_rank(top: int = 100) -> pd.DataFrame:
    import sys as _sys
    import os as _os

    adata_path = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), *[".."] * 4, "adata"))
    if adata_path not in _sys.path:
        _sys.path.insert(0, adata_path)
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


def _live_fused_rank(top: int = 100) -> dict:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tools.merge_hot_rank import _attach_industry, _filter_hs_a, _fuse_single_day, _load_industry_map

    # 缓存 60 秒，避免频繁切换页面时重复请求外部 API
    cache_key = f"fused_live_{top}"
    cached = _cache_get(cache_key, ttl_seconds=60)
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

@router.get("/hot-data/latest-trade-date")
def latest_trade_date():
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
    except Exception:
        return {"latest_date": date.today().isoformat()}


def _fallback_date(table: str, col: str, requested: str) -> str:
    try:
        rows = _read_sql(
            f"SELECT {col} AS d FROM {table} WHERE {col} <= :d ORDER BY {col} DESC LIMIT 1",
            {"d": requested},
        )
        if rows and rows[0].get("d"):
            return str(rows[0]["d"])
    except Exception:
        pass
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
            rows = _read_sql(f"SELECT DISTINCT {col} AS d FROM {tbl} ORDER BY {col} DESC")
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
def fused_live(top: int = Query(default=100, ge=1, le=200)):
    """盘中实时融合榜：直接抓取东财/同花顺/雪球/新浪热股并即时融合，不落库。"""
    try:
        return _live_fused_rank(top)
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
        rows = _read_sql("SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d ORDER BY plate_type, rank", {"d": snapshot_date})
        if not rows:
            fb = _fallback_date("st_hot_concept_ths_daily", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d ORDER BY plate_type, rank", {"d": fb})
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
        import sys as _sys
        import os as _os
        from concurrent.futures import ThreadPoolExecutor
        from datetime import datetime as _dt
        _adata = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), *[".."]*4, "adata"))
        if _adata not in _sys.path:
            _sys.path.insert(0, _adata)
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
        rows = _read_sql("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY rank LIMIT :n", {"d": snapshot_date, "n": top})
        if not rows:
            fb = _fallback_date("st_hot_rank_ths", "snapshot_date", snapshot_date)
            if fb != snapshot_date:
                rows = _read_sql("SELECT * FROM st_hot_rank_ths WHERE snapshot_date = :d ORDER BY rank LIMIT :n", {"d": fb, "n": top})
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
            ORDER BY e.rank LIMIT :n
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
                    ORDER BY e.rank LIMIT :n
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
            r = c.get(url, params=params)
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
        rows = _read_sql("SELECT id, trade_date, stock_code, operate_name, a_net_amount, a_buy_amount, a_sell_amount, a_buy_amount_rate, a_sell_amount_rate, reason FROM st_a_list_info WHERE trade_date = :d AND stock_code = :c ORDER BY a_net_amount DESC", {"d": trade_date, "c": stock_code})
        return {"date": trade_date, "stock_code": stock_code, "data": rows, "total": len(rows)}
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
    sql = f"SELECT f.id, f.stock_code, COALESCE(s.short_name, '') AS short_name, f.trade_date, f.main_net_inflow, f.max_net_inflow, f.lg_net_inflow, f.mid_net_inflow, f.sm_net_inflow, f.data_source FROM sm_stock_capital_flow_daily f LEFT JOIN si_all_code s ON f.stock_code = s.stock_code WHERE {where} ORDER BY f.main_net_inflow {order}"
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
    except Exception:
        pass
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
        from adata.stock.market.capital_flow.stock_capital_flow import StockCapitalFlow
        cf = StockCapitalFlow()
        df = cf.get_capital_flow_min(stock_code=stock_code.strip())
        if df is None or df.empty:
            return {"stock_code": stock_code, "data": [], "latest": None, "total": 0}
        import numpy as np
        df = df.replace({np.nan: None, pd.NaT: None})
        latest = df.iloc[-1].to_dict()
        latest["trade_time"] = str(latest["trade_time"])
        records = df.to_dict(orient="records")
        for r in records:
            r["trade_time"] = str(r["trade_time"])
        return {"stock_code": stock_code, "data": records, "latest": latest, "total": len(records)}
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
            except Exception:
                pass
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
            except Exception:
                pass
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


@router.get("/hot-data/concept-multi-day")
def concept_multi_day(stat_date: str = Query(default_factory=lambda: date.today().isoformat()), days: int = 3, plate_type: int = 0):
    """近N天热门概念/行业聚合"""
    try:
        import numpy as np
        is_fallback = False
        if days <= 1:
            rows = _read_sql(
                "SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d" +
                (" AND plate_type = :p" if plate_type > 0 else "") + " ORDER BY plate_type, rank",
                {"d": stat_date, "p": plate_type} if plate_type > 0 else {"d": stat_date}
            )
            if not rows:
                fb = _fallback_date("st_hot_concept_ths_daily", "snapshot_date", stat_date)
                if fb != stat_date:
                    rows = _read_sql(
                        "SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = :d" +
                        (" AND plate_type = :p" if plate_type > 0 else "") + " ORDER BY plate_type, rank",
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
                f"SELECT snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value, hot_tag FROM st_hot_concept_ths_daily WHERE snapshot_date = :d{plate_sql}",
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
                        f"SELECT snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value, hot_tag FROM st_hot_concept_ths_daily WHERE snapshot_date = :d{plate_sql}",
                        params
                    )
                    all_rows.extend(batch)
                if all_rows:
                    stat_date = fb
                    is_fallback = True
            if not all_rows:
                return {"date": stat_date, "days": days, "data": [], "total": 0}

        df = pd.DataFrame(all_rows)
        # 按概念代码聚合
        grouped = df.groupby(["concept_code", "concept_name", "plate_type"]).agg(
            appear_days=("snapshot_date", "nunique"),
            avg_rank=("rank", "mean"),
            best_rank=("rank", "min"),
            avg_change_pct=("change_pct", "mean"),
            avg_hot_value=("hot_value", "mean"),
            last_rank=("rank", "last"),
            last_change_pct=("change_pct", "last"),
            last_hot_value=("hot_value", "last"),
        ).reset_index()

        grouped["appear_pct"] = (grouped["appear_days"] / days * 100).round(1)
        grouped = grouped.sort_values(["appear_days", "avg_rank"], ascending=[False, True])
        grouped = grouped.replace({np.nan: None, np.inf: None})
        result = grouped.to_dict(orient="records")
        for r in result:
            for k in ["avg_rank", "best_rank", "avg_change_pct", "avg_hot_value", "last_rank", "last_change_pct", "last_hot_value"]:
                if isinstance(r.get(k), float):
                    r[k] = round(r[k], 2) if abs(r[k]) < 1e15 else None

        resp = {"date": stat_date, "days": days, "data": result, "total": len(result)}
        if is_fallback:
            resp["fallback"] = True
        return resp
    except Exception as e:
        return {"date": stat_date, "days": days, "data": [], "total": 0, "error": str(e)}


def _fetch_cls_news(client, pages=2):
    import re as _re
    items = []
    last_time = 0
    for _ in range(pages):
        url = "https://www.cls.cn/nodeapi/updateTelegraphList?app=CailianpressWeb&os=web&sv=8.4.6&rn=50"
        if last_time:
            url += f"&last_time={last_time}"
        r = client.get(url)
        r.raise_for_status()
        roll_data = (r.json().get("data") or {}).get("roll_data") or []
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
        r = client.get(url)
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
                except Exception:
                    pass
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
        r = client.get(url)
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
                except Exception:
                    pass
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
                except Exception:
                    pass
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
        "INSERT INTO st_news_flash (source, source_id, title, content, publish_time, level, stocks, subjects, reading_num, is_top, jpush, extra, etl_sync_at) "
        "VALUES (:source, :source_id, :title, :content, :publish_time, :level, :stocks, :subjects, :reading_num, :is_top, :jpush, :extra, :etl_sync_at) "
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
                except Exception:
                    pass
    except Exception:
        pass
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
                except Exception:
                    pass
            if source in ("all", "eastmoney"):
                try:
                    all_items.extend(_fetch_eastmoney_news(client, max(1, pages // 2)))
                except Exception:
                    pass
            if source in ("all", "sina"):
                try:
                    all_items.extend(_fetch_sina_news(client, max(1, pages // 2)))
                except Exception:
                    pass

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
            except Exception:
                pass
            try:
                all_items.extend(_fetch_eastmoney_news(client, max(1, pages // 2)))
            except Exception:
                pass
            try:
                all_items.extend(_fetch_sina_news(client, max(1, pages // 2)))
            except Exception:
                pass

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
def stock_notices(stock_code: str = Query(default=""), limit: int = Query(default=50)):
    """查询个股公告（si_notice_eastmoney）"""
    try:
        if stock_code.strip():
            rows = _read_sql(
                "SELECT stock_code, notice_date, title, column_name, display_time, detail_url "
                "FROM si_notice_eastmoney WHERE stock_code = :c ORDER BY notice_date DESC LIMIT :n",
                {"c": stock_code.strip().zfill(6), "n": min(limit, 200)}
            )
        else:
            rows = _read_sql(
                "SELECT stock_code, notice_date, title, column_name, display_time, detail_url "
                "FROM si_notice_eastmoney ORDER BY notice_date DESC LIMIT :n",
                {"n": min(limit, 200)}
            )
        return {"data": rows, "total": len(rows)}
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
    try:
        rows = _read_sql(f"SELECT MAX({col}) AS md FROM {table}", {})
        if rows and rows[0].get("md"):
            return str(rows[0]["md"])
    except Exception:
        pass
    return date.today().isoformat()


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
    trend_days: int = Query(default=10, alias="t_days", description="[trend_strong] 连续站上MA5最少天数"),
    ma_slope_min: float = Query(default=0.5, alias="slope", description="[trend_strong] MA20斜率下限%"),
    vol_ratio_min: float = Query(default=0.8, alias="vr_min", description="[trend_strong] 量比下限"),
    vol_ratio_max: float = Query(default=2.5, alias="vr_max", description="[trend_strong] 量比上限"),
    max_60d_gain: float = Query(default=150.0, alias="max_gain", description="[trend_strong] 60日最大涨幅%"),
    new_high_pct: float = Query(default=0.95, alias="nh_pct", description="[trend_strong] 距新高比例"),
):
    """选股策略筛选（自动兜底最新可用日期）"""

    try:
        if mode == "lhb":
            td = _latest_date("st_a_list_daily")
            if not _read_sql(f"SELECT 1 FROM st_a_list_daily WHERE trade_date=:d LIMIT 1", {"d": td}):
                return {"mode": mode, "date": td, "data": [], "total": 0, "note": "龙虎榜无数据"}
            sql = """
                SELECT d.stock_code, COALESCE(NULLIF(d.short_name,''), c.short_name) AS short_name,
                       d.change_cpt AS change_pct, d.turnover_ratio,
                       d.a_net_amount, d.reason
                FROM st_a_list_daily d LEFT JOIN si_all_code c ON c.stock_code = d.stock_code
                WHERE d.trade_date = :d ORDER BY ABS(d.change_cpt) DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "lim": top})
        elif mode == "flow":
            td = _latest_date("sm_stock_capital_flow_daily")
            sql = """
                SELECT f.stock_code, c.short_name, f.main_net_inflow, f.max_net_inflow
                FROM sm_stock_capital_flow_daily f LEFT JOIN si_all_code c ON c.stock_code = f.stock_code
                WHERE f.trade_date = :d AND f.main_net_inflow >= :m
                ORDER BY f.main_net_inflow DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "m": min_main_flow, "lim": top})
        elif mode == "k_day":
            td = _latest_date("sm_stock_kline")
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
            td = _latest_date("sm_stock_kline")
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
            td = _latest_date("sm_stock_kline")
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
                  AND t.stock_code REGEXP '^(0|60)'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND ma5.avg_c > ma10.avg_c AND ma10.avg_c > ma20.avg_c AND t.close > ma5.avg_c
                  AND t.change_pct >= :cmin
                ORDER BY ma_spread_pct DESC LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "cmin": min_chg_trend, "lim": top})
        elif mode == "trend_strong":
            td = _latest_date("sm_stock_kline")
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
                  AND t.stock_code REGEXP '^(0|60)'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%%ST%%'
                  AND t.short_name NOT LIKE '%%ST%%'
                  AND ma5.v > ma10.v AND ma10.v > ma20.v AND ma20.v > ma60.v
                  AND t.close > ma5.v
                ORDER BY t.close / NULLIF(ma60.v, 0) DESC
                LIMIT 800
            """
            raw_rows = _read_sql(sql, {"d": td})
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
            td = _latest_date("sm_stock_kline")
            # 第一步: 获取今日涨停股
            today_limit = _read_sql("""
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.change_pct >= :pct
            """, {"d": td, "pct": limit_pct})
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
                    boards = 1
                    for h in hist_map.get(code, []):
                        if str(h["trade_date"]) == str(td):
                            continue
                        if float(h["change_pct"] or 0) >= limit_pct:
                            boards += 1
                        else:
                            break
                    if min_boards <= boards <= max_boards:
                        r["boards"] = boards
                        rows.append(r)
                rows.sort(key=lambda x: (-x["boards"], -float(x["change_pct"] or 0)))
                rows = rows[:top]
        elif mode == "macd":
            td = _latest_date("sm_stock_kline")
            sql = """
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio,
                       ROUND(ma12.avg_c,2) AS ema12, ROUND(ma26.avg_c,2) AS ema26,
                       ROUND(ma12.avg_c - ma26.avg_c, 2) AS dif
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 12 DAY) AND :d
                  GROUP BY stock_code
                ) ma12 ON t.stock_code = ma12.stock_code
                INNER JOIN (
                  SELECT stock_code, AVG(close) AS avg_c FROM sm_stock_kline
                  WHERE k_type=1 AND adjust_type=0 AND trade_date BETWEEN DATE_SUB(:d, INTERVAL 26 DAY) AND :d
                  GROUP BY stock_code
                ) ma26 ON t.stock_code = ma26.stock_code
                WHERE t.trade_date = :d AND t.k_type=1 AND t.adjust_type=0
                  AND t.stock_code REGEXP '^(0|60)'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%*ST%'
                  AND ma12.avg_c > ma26.avg_c
                ORDER BY (ma12.avg_c - ma26.avg_c) / NULLIF(ma26.avg_c, 0) * 100 DESC
                LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "lim": top})
            for r in (rows or []):
                r["dea"] = round(r.get("dif", 0) * 0.9, 2)
                r["hist"] = round((r.get("dif", 0) - r.get("dea", 0)) * 2, 2)
                r["k"] = 50
                r["d"] = 50
                r["j"] = 50
        elif mode == "startup":
            td = _latest_date("sm_stock_kline")
            sql = """
                SELECT t.stock_code, COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
                       ROUND(t.close,2) AS close, t.change_pct, t.turnover_ratio,
                       ROUND(ma20.avg_c,2) AS ma20,
                       ROUND(t.volume / NULLIF(base.avg_vol, 0), 1) AS vol_ratio,
                       ROUND((t.close - base.max_high) / NULLIF(base.max_high, 0) * 100, 1) AS breakout_pct,
                       ROUND((base.max_high - base.min_low) / NULLIF(base.min_low, 0) * 100, 1) AS range_width_pct,
                       COALESCE(f.main_net_inflow, 0) AS main_net_inflow,
                       COALESCE(f2.main_net_inflow, 0) AS prev_flow
                FROM sm_stock_kline t
                LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
                LEFT JOIN sm_stock_capital_flow_daily f ON t.stock_code = f.stock_code AND f.trade_date = :d
                LEFT JOIN sm_stock_capital_flow_daily f2 ON t.stock_code = f2.stock_code
                  AND f2.trade_date = (SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily
                                       WHERE stock_code = t.stock_code AND trade_date < :d)
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
                  AND t.stock_code REGEXP '^(0|60)'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%ST%'
                  AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%*ST%'
                  AND t.close > ma20.avg_c
                  AND t.close < ma20.avg_c * 1.2
                  AND t.volume > base.avg_vol * 1.3
                  AND t.close >= base.max_high
                  AND (base.max_high - base.min_low) / NULLIF(base.min_low, 0) < 0.18
                ORDER BY COALESCE(f.main_net_inflow, 0) DESC, t.volume / NULLIF(base.avg_vol, 0) DESC
                LIMIT :lim
            """
            rows = _read_sql(sql, {"d": td, "lim": top})
            if rows:
                codes = [str(r["stock_code"]) for r in rows]
                inds = _compute_indicators(codes, td)
                news_map = {}
                for code in codes:
                    try:
                        cnt_rows = _read_sql(
                            "SELECT COUNT(*) AS cnt FROM st_news_flash "
                            "WHERE publish_time >= DATE_SUB(:d, INTERVAL 3 DAY) "
                            "AND stocks LIKE :kw",
                            {"d": td, "kw": "%" + code + "%"}
                        )
                        if cnt_rows:
                            news_map[code] = cnt_rows[0]["cnt"]
                    except Exception:
                        pass
                for r in rows:
                    ind = inds.get(str(r["stock_code"]), {})
                    r["k"] = ind.get("k", 50)
                    r["d"] = ind.get("d", 50)
                    r["j"] = ind.get("j", 50)
                    r["dif"] = ind.get("dif", 0)
                    r["dea"] = ind.get("dea", 0)
                    r["macd_golden"] = ind.get("golden_cross", False)
                    r["news_count"] = news_map.get(str(r["stock_code"]), 0)
        else:
            return {"mode": mode, "data": [], "total": 0, "error": f"未知模式: {mode}"}

        return {"mode": mode, "date": td if 'td' in dir() else trade_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"mode": mode, "date": trade_date, "data": [], "total": 0, "error": str(e)}


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
        rows = _read_sql(f"SELECT MAX({col}) AS md FROM {table}", {})
        if rows and rows[0].get("md"):
            return str(rows[0]["md"])
    except Exception:
        pass
    return date.today().isoformat()


@router.get("/hot-data/stock-minute")
def stock_minute(stock_code: str = Query(), trade_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """获取个股当天分时走势"""
    try:
        rows = _read_sql(
            "SELECT trade_time, price, avg_price, change, change_pct, volume, amount FROM sm_stock_minute "
            "WHERE stock_code = :c AND trade_date = :d ORDER BY trade_time",
            {"c": stock_code, "d": trade_date}
        )
        if rows:
            return {"stock_code": stock_code, "date": trade_date, "data": rows, "total": len(rows), "source": "db"}
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
        except Exception:
            pass
        return {"stock_code": stock_code, "date": trade_date, "data": [], "total": 0, "source": "none"}
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
                s.change, s.change_pct, s.volume, s.amount, s.turnover_ratio,
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

        # 分页查询
        offset = (page - 1) * page_size
        query_sql = f"""
            SELECT stock_code, stock_name, analysis_date, last_news_time,
                   long_term_score, fundamental_score, growth_score, valuation_score, risk_score,
                   short_term_score, capital_score, technical_score, sentiment_score, event_score,
                   event_risk_score, event_risk_level, event_risk_detail,
                   recommend_status, recommend_reason,
                   summary, recommendation, strengths, risks
            FROM stock_analysis_result
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
            for json_field in ["event_risk_detail", "strengths", "risks"]:
                if row.get(json_field) and isinstance(row[json_field], str):
                    try:
                        row[json_field] = json.loads(row[json_field])
                    except:
                        pass

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
    try:
        code = stock_code.strip().zfill(6)
        td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = td_rows[0]["d"] if td_rows and td_rows[0].get("d") else date.today().isoformat()
        mode = _portfolio_market_mode()

        # ─── 基本信息 ───
        basic_rows = _read_sql(
            "SELECT stock_code, short_name, exchange, list_date FROM si_all_code WHERE stock_code = :c",
            {"c": code}
        )
        if not basic_rows:
            return {"error": f"股票 {code} 不存在"}
        basic = basic_rows[0]

        # 行业
        industry_rows = _read_sql(
            "SELECT plate_name FROM si_stock_plate_east WHERE stock_code = :c AND plate_type = '行业'",
            {"c": code}
        )
        if industry_rows and industry_rows[0].get("plate_name"):
            industry = industry_rows[0]["plate_name"]
        else:
            sw_rows = _read_sql(
                "SELECT industry_name FROM si_industry_sw WHERE stock_code = :c AND industry_type = '申万一级'",
                {"c": code}
            )
            industry = sw_rows[0]["industry_name"] if sw_rows and sw_rows[0].get("industry_name") else None

        # 概念
        concept_rows = _read_sql(
            "SELECT DISTINCT name FROM si_stock_concept_east WHERE stock_code = :c LIMIT 20",
            {"c": code}
        )
        concepts = [r["name"] for r in concept_rows if r.get("name")]

        # ─── 一、行情数据 ───
        quote = {}
        if mode == "intraday":
            cur_rows = _read_sql(
                "SELECT price, change_pct, snapshot_at FROM sm_stock_current WHERE stock_code = :c",
                {"c": code}
            )
            if cur_rows and cur_rows[0].get("price") is not None:
                quote = {**cur_rows[0], "source": "realtime"}
        if not quote:
            kline_rows = _read_sql(
                "SELECT close AS price, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close "
                "FROM sm_stock_kline WHERE stock_code = :c AND trade_date = :td AND k_type=1",
                {"c": code, "td": trade_date}
            )
            if kline_rows:
                quote = {**kline_rows[0], "source": "kline"}
        if quote.get("high") and quote.get("low") and quote.get("open") and float(quote.get("open", 0)) > 0:
            quote["amplitude"] = round((float(quote["high"]) - float(quote["low"])) / float(quote["open"]) * 100, 2)

        # 股本 + 市值
        cap_rows = _read_sql(
            "SELECT total_shares, limit_shares, list_a_shares FROM si_stock_shares WHERE stock_code = :c",
            {"c": code}
        )
        cap = cap_rows[0] if cap_rows else {}
        price_val = float(quote.get("price") or 0)
        total_shares = float(cap.get("total_shares") or 0)
        float_shares = float(cap.get("list_a_shares") or 0)

        # 量比（今日成交量 / 近5日平均成交量）
        vol_rows = _read_sql("""
            SELECT AVG(volume) AS avg_vol FROM (
                SELECT volume FROM sm_stock_kline
                WHERE stock_code = :c AND k_type=1 AND trade_date < :td
                ORDER BY trade_date DESC LIMIT 5
            ) t
        """, {"c": code, "td": trade_date})
        avg_vol = float(vol_rows[0]["avg_vol"]) if vol_rows and vol_rows[0].get("avg_vol") else 0
        cur_vol = float(quote.get("volume") or 0)
        volume_ratio = round(cur_vol / avg_vol, 2) if avg_vol > 0 else None

        # 财务指标（PE/PB/ROE等，取最新报告期）
        fin_rows = _read_sql("""
            SELECT basic_eps, net_asset_ps, roe_wtd, roa_wtd, gross_margin, net_margin,
                   total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr,
                   report_date
            FROM si_stock_finance WHERE stock_code = :c
            ORDER BY report_date DESC LIMIT 1
        """, {"c": code})
        fin = fin_rows[0] if fin_rows else {}
        eps = float(fin.get("basic_eps") or 0)
        bvps = float(fin.get("net_asset_ps") or 0)
        pe_ttm = round(price_val / eps, 2) if eps and eps > 0 and price_val else None
        pb = round(price_val / bvps, 2) if bvps and bvps > 0 and price_val else None

        market = {
            "price": quote.get("price"),
            "change_pct": quote.get("change_pct"),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "close": quote.get("close") or quote.get("price"),
            "pre_close": quote.get("pre_close"),
            "volume": quote.get("volume"),
            "amount": quote.get("amount"),
            "turnover_ratio": quote.get("turnover_ratio"),
            "amplitude": quote.get("amplitude"),
            "pe_ttm": pe_ttm,
            "pb": pb,
            "volume_ratio": volume_ratio,
            "total_shares": total_shares,
            "float_shares": float_shares,
            "market_cap": round(price_val * total_shares, 2) if price_val and total_shares else None,
            "float_market_cap": round(price_val * float_shares, 2) if price_val and float_shares else None,
        }

        # ─── 二、资金面 ───
        # 今日资金流向
        flow_td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily")
        flow_td = flow_td_rows[0]["d"] if flow_td_rows and flow_td_rows[0].get("d") else trade_date
        flow_rows = _read_sql(
            "SELECT main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source "
            "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :ftd",
            {"c": code, "ftd": flow_td}
        )
        flow_today = flow_rows[0] if flow_rows else {}

        # 近3/5/20日资金流向累计
        flow_multi = {}
        for days, label in [(3, "flow_3d"), (5, "flow_5d"), (20, "flow_20d")]:
            mf = _read_sql(f"""
                SELECT SUM(main_net_inflow) AS main_net_inflow
                FROM sm_stock_capital_flow_daily
                WHERE stock_code = :c AND trade_date >= (
                    SELECT trade_date FROM sm_stock_kline WHERE k_type=1 AND trade_date <= :td
                    GROUP BY trade_date ORDER BY trade_date DESC LIMIT 1 OFFSET {days - 1}
                )
            """, {"c": code, "td": flow_td})
            flow_multi[label] = mf[0]["main_net_inflow"] if mf and mf[0].get("main_net_inflow") else None

        # 龙虎榜（近20日）
        lhb_rows = _read_sql("""
            SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy
            FROM st_a_list_daily
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
        """, {"c": code, "td": trade_date})
        lhb = lhb_rows[0] if lhb_rows else {}

        lhb_seats = _read_sql("""
            SELECT trade_date, operate_name, a_net_amount, a_buy_amount, a_sell_amount
            FROM st_a_list_info
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
            ORDER BY trade_date DESC LIMIT 10
        """, {"c": code, "td": trade_date})

        capital = {
            "today": flow_today,
            "flow_3d": flow_multi.get("flow_3d"),
            "flow_5d": flow_multi.get("flow_5d"),
            "flow_20d": flow_multi.get("flow_20d"),
            "dragon_tiger": {
                "count_20d": int(lhb.get("cnt") or 0),
                "inst_net_buy": lhb.get("inst_net_buy"),
                "seats": lhb_seats,
            },
        }

        # ─── 股东人数 ───
        holder_rows = _read_sql("""
            SELECT report_date, holder_num, holder_num_change, pre_holder_num,
                   holder_num_ratio, avg_free_shares
            FROM si_stock_holder WHERE stock_code = :c
            ORDER BY report_date DESC LIMIT 2
        """, {"c": code})
        holder = {}
        if holder_rows:
            h0 = holder_rows[0]
            holder = {
                "report_date": str(h0.get("report_date", "")),
                "holder_num": int(h0["holder_num"]) if h0.get("holder_num") is not None else None,
                "holder_num_change": int(h0["holder_num_change"]) if h0.get("holder_num_change") is not None else None,
                "pre_holder_num": int(h0["pre_holder_num"]) if h0.get("pre_holder_num") is not None else None,
                "holder_num_ratio": float(h0["holder_num_ratio"]) if h0.get("holder_num_ratio") is not None else None,
                "avg_free_shares": float(h0["avg_free_shares"]) if h0.get("avg_free_shares") is not None else None,
            }

        # ─── 三、财务面 ───
        # 最近8个报告期
        fin_rows = _read_sql("""
            SELECT report_date, report_type, basic_eps, net_asset_ps,
                   total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr,
                   roe_wtd, roa_wtd, gross_margin, net_margin,
                   curr_ratio, quick_ratio, asset_liab_ratio
            FROM si_stock_finance WHERE stock_code = :c
            ORDER BY report_date DESC LIMIT 8
        """, {"c": code})

        finance = {
            "latest": fin or {},
            "quarters": fin_rows,
        }

        # ─── 四、估值面 ───
        # PE/PB 历史分位（基于所有报告期的EPS/BVPS计算历史PE/PB）
        pe_history = _read_sql("""
            SELECT f.report_date, f.basic_eps, f.net_asset_ps
            FROM si_stock_finance f
            WHERE f.stock_code = :c AND f.basic_eps > 0
            ORDER BY f.report_date DESC LIMIT 20
        """, {"c": code})

        pe_percentile = None
        pb_percentile = None
        if pe_history and pe_ttm:
            pe_list = []
            for r in pe_history:
                e = float(r.get("basic_eps") or 0)
                if e > 0:
                    pe_list.append(price_val / e)
            if pe_list:
                below = sum(1 for p in pe_list if p <= pe_ttm)
                pe_percentile = round(below / len(pe_list) * 100, 1)

        if pe_history and pb:
            pb_list = []
            for r in pe_history:
                b = float(r.get("net_asset_ps") or 0)
                if b > 0:
                    pb_list.append(price_val / b)
            if pb_list:
                below = sum(1 for p in pb_list if p <= pb)
                pb_percentile = round(below / len(pb_list) * 100, 1)

        valuation = {
            "pe_ttm": pe_ttm,
            "pe_percentile": pe_percentile,
            "pb": pb,
            "pb_percentile": pb_percentile,
            "verdict": "偏高" if (pe_percentile and pe_percentile > 70) else "偏低" if (pe_percentile and pe_percentile < 30) else "合理" if pe_percentile else None,
        }

        # ─── 五、技术面 ───
        # 取近250日K线计算技术指标
        kline_250 = _read_sql("""
            SELECT trade_date, open, close, high, low, volume, change_pct
            FROM sm_stock_kline WHERE stock_code = :c AND k_type=1
            ORDER BY trade_date DESC LIMIT 260
        """, {"c": code})

        technical = _compute_technical(kline_250, price_val)

        # ─── 六、消息面 ───
        notices = _read_sql("""
            SELECT notice_date, title, column_name, detail_url
            FROM si_notice_eastmoney WHERE stock_code = :c
            ORDER BY notice_date DESC LIMIT 10
        """, {"c": code})

        news = _read_sql("""
            SELECT publish_time, title, source
            FROM st_news_flash
            WHERE stocks LIKE :kw
            ORDER BY publish_time DESC LIMIT 10
        """, {"kw": f"%{code}%"})

        news_module = {
            "notices": notices,
            "news": news,
        }

        # ─── 七、AI分析（使用统一分析引擎） ───
        # 持仓信息
        holding_rows = _read_sql(
            "SELECT shares, cost_price FROM st_user_portfolio WHERE stock_code = :c AND shares > 0",
            {"c": code}
        )
        holding = holding_rows[0] if holding_rows else None

        # AI分析：优先使用 DeepSeek 生成详细文字分析
        ai_analysis = _generate_ai_analysis(code, basic.get("short_name"), market, capital, finance, valuation, technical, industry, concepts, holding)

        return {
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
            "ai_analysis": ai_analysis,
            "holding": holding,
            "mode": mode,
            "date": trade_date,
        }
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


def _generate_ai_analysis(code, name, market, capital, finance, valuation, technical, industry, concepts, holding=None):
    """调用 DeepSeek 生成 AI 投资分析"""
    try:
        import httpx
        api_key = os.getenv("DEEPSEEK_API_KEY", "")
        if not api_key:
            return {"score": None, "conclusion": "未配置 DEEPSEEK_API_KEY"}

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
            hot_rows = _read_sql("""
                SELECT fused_rank, east_rank, ths_rank, total_score
                FROM st_hot_rank_fused
                WHERE stock_code = :c
                ORDER BY snapshot_date DESC LIMIT 1
            """, {"c": code})
            if hot_rows:
                hr = hot_rows[0]
                summary_parts.append(f"融合热度排名：第{hr.get('fused_rank')}名，东方财富排名：{hr.get('east_rank')}，同花顺排名：{hr.get('ths_rank')}")
        except Exception:
            pass

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

请严格按照以下格式返回分析文本（不要返回JSON，直接返回文本）：

### 操作建议：[一句话操作建议，如"持有观望，暂不加仓"或"逢低加仓"或"减仓锁定利润"]

趋势判断：[结合均线、量能、波动率分析当前走势格局，要引用具体数字如MA5价格、涨跌幅等]

资金态度：[分析主力资金流向，引用具体金额如"主力净流入XX亿"，判断资金态度]

热度评估：[分析市场关注度、概念热度、排名等]

操作建议：[如有持仓，给出详细的持仓操作建议，引用成本价、现价、盈亏比例、支撑压力位；如无持仓，给出买入/观望建议]

风险提示：[列出2-3个具体风险点，要具体不要泛泛而谈]"""

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
        return {"conclusion": content.strip()}
    except Exception as e:
        return {"conclusion": f"AI分析生成失败: {str(e)}"}


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


@router.get("/hot-data/daily-review-dates")
def daily_review_dates():
    """复盘数据可用日期列表"""
    try:
        rows = _read_sql("SELECT DISTINCT review_date AS d FROM st_daily_review ORDER BY review_date DESC")
        return {"dates": [r["d"] for r in rows]}
    except Exception as e:
        return {"dates": [], "error": str(e)}


@router.post("/hot-data/daily-review/generate")
def generate_daily_review(review_date: str = Query(default_factory=lambda: date.today().isoformat())):
    """生成复盘数据"""
    try:
        import subprocess
        import sys
        _ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        cmd = [sys.executable, "-m", "biz.review.generate", review_date]
        child_env = os.environ.copy()
        child_env.setdefault("PYTHONPATH", _ROOT + ":" + os.path.join(_ROOT, "adata"))
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=_ROOT, env=child_env)
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
                try: return json.loads(v)
                except Exception: return []
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
            c = cold[i] if i < len(cold) else None
            html += "<tr>"
            if h and isinstance(h, dict):
                chg = h.get("change_pct")
                html += f"<td>{h.get('name','-')}</td><td class=\"{'up' if (chg or 0) >= 0 else 'down'}\">{chg:+.2f}%</td>"
            else:
                html += "<td></td><td></td>"
            if c and isinstance(c, dict):
                chg = c.get("change_pct")
                html += f"<td>{c.get('name','-')}</td><td class=\"{'up' if (chg or 0) >= 0 else 'down'}\">{chg:+.2f}%</td>"
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
    """导出复盘数据为文本（保留兼容）"""
    try:
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


# ═══════════════════════════════════════════
# 自选股持仓 API
# ═══════════════════════════════════════════

from pydantic import BaseModel

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
    except Exception:
        pass


def _portfolio_ensure_column(table: str, column: str, ddl: str) -> None:
    try:
        rows = _read_sql("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = :t AND column_name = :c
        """, {"t": table, "c": column})
        if not rows or int(rows[0].get("cnt") or 0) == 0:
            _exec_sql(ddl)
    except Exception:
        pass


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
    except Exception:
        pass


def _portfolio_log_trans(code: str, trans_type: str, price: float, shares: int, source: str = "trade"):
    _ensure_portfolio_trans_log_table()
    _exec_sql("""
        INSERT INTO st_portfolio_trans_log (stock_code, trans_type, price, shares, source, trans_date, created_at)
        VALUES (:c, :t, :p, :s, :src, CURDATE(), NOW())
    """, {"c": code, "t": trans_type, "p": price, "s": shares, "src": source})


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
    if quote.get("source"):
        row["quote_source"] = quote.get("source")


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


@router.get("/portfolio/list")
def portfolio_list():
    """持仓列表（带实时行情）"""
    try:
        _ensure_portfolio_position_columns()
        rows = _read_sql("""
            SELECT p.*,
                   COALESCE(s.price, k.close) AS cur_price,
                   s.price AS live_price,
                   COALESCE(s.`change`, k.close - k.pre_close) AS price_change,
                   COALESCE(s.change_pct, k.change_pct) AS change_pct,
                   COALESCE(s.short_name, k.short_name) AS current_name,
                    k.pre_close AS kline_pre_close,
                    k.trade_date AS kline_trade_date
            FROM st_user_portfolio p
            LEFT JOIN sm_stock_current s ON s.stock_code = p.stock_code COLLATE utf8mb4_general_ci
            LEFT JOIN (
                 SELECT k1.stock_code, k1.trade_date, k1.close, k1.change_pct, k1.short_name, k1.pre_close
                 FROM sm_stock_kline k1
                 INNER JOIN (
                     SELECT stock_code, MAX(trade_date) AS max_d
                     FROM sm_stock_kline
                     WHERE k_type = 1 AND stock_code IN (SELECT stock_code COLLATE utf8mb4_general_ci FROM st_user_portfolio)
                     GROUP BY stock_code
                 ) k2 ON k1.stock_code = k2.stock_code AND k1.trade_date = k2.max_d
                 WHERE k1.k_type = 1
             ) k ON k.stock_code = p.stock_code COLLATE utf8mb4_general_ci
            ORDER BY (p.shares > 0) DESC, p.sort_order, p.id
        """)
        live_quotes = {}
        codes = [str(r.get("stock_code") or "").strip().zfill(6) for r in rows if r.get("stock_code")]
        if codes:
            try:
                live_quotes = _portfolio_fetch_live_quotes(codes)
            except Exception:
                live_quotes = {}
        total_hold_profit = 0.0
        today_hold_profit = 0.0
        holding_count = 0
        today_open_count = 0
        today_cleared_count = 0
        for r in rows:
            code = str(r.get("stock_code") or "").strip().zfill(6)
            _portfolio_apply_quote(r, live_quotes.get(code))
            trade_date = _portfolio_quote_trade_date(r)
            r["quote_trade_date"] = trade_date
            cp = float(r.get("cost_price") or 0)
            pr = float(r.get("cur_price") or 0) if r.get("cur_price") is not None else 0.0
            sh = int(r.get("shares") or 0)
            trades = _portfolio_effective_today_trades(code, sh, cp, r, trade_date)
            trade_state = _portfolio_today_trade_state(sh, trades)
            r.update(trade_state)
            r["is_holding"] = sh > 0
            hold_profit = _portfolio_cost_profit(sh, pr, cp)
            r["profit"] = hold_profit
            r["profit_pct"] = round((pr / cp - 1) * 100, 2) if sh > 0 and pr > 0 and cp > 0 else None
            r["today_profit"] = _portfolio_day_profit(
                sh,
                pr,
                r.get("live_price"),
                r.get("price_change"),
                r.get("kline_pre_close"),
                r.get("change_pct"),
                r["stock_code"],
                trades,
                trade_date,
            )
            r["display_name"] = r.get("current_name") or r.get("short_name") or r["stock_code"]
            if r.get("is_today_open") or r.get("is_today_reopened"):
                today_open_count += 1
            if r.get("is_today_cleared"):
                today_cleared_count += 1
            if sh > 0:
                holding_count += 1
                if hold_profit is not None:
                    total_hold_profit += hold_profit
            if r["today_profit"] is not None:
                today_hold_profit += r["today_profit"]
        return {
            "data": rows,
            "total": len(rows),
            "summary": {
                "holding_count": holding_count,
                "total_hold_profit": round(total_hold_profit, 2),
                "today_hold_profit": round(today_hold_profit, 2),
                "today_open_count": today_open_count,
                "today_cleared_count": today_cleared_count,
            },
        }
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


@router.post("/portfolio/add")
def portfolio_add(body: PortfolioAdd):
    """添加自选股"""
    try:
        _ensure_portfolio_position_columns()
        _ensure_portfolio_trans_log_table()
        code = body.stock_code.strip().zfill(6)
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
        return {"status": "ok", "stock_code": code, "short_name": name}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.delete("/portfolio/remove/{stock_code}")
def portfolio_remove(stock_code: str):
    """删除自选股"""
    try:
        code = stock_code.strip().zfill(6)
        _exec_sql("DELETE FROM st_user_portfolio WHERE stock_code = :c", {"c": code})
        # 同步更新快照表：移除自选股排序和持仓标记
        _exec_sql("UPDATE sm_stock_snapshot SET sort_order = NULL, is_holding = 0 WHERE stock_code = :c", {"c": code})
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


class PortfolioReorder(BaseModel):
    codes: list[str] = []


@router.post("/portfolio/reorder")
def portfolio_reorder(body: PortfolioReorder):
    """拖拽排序"""
    try:
        for i, code in enumerate(body.codes):
            c = code.strip().zfill(6)
            _exec_sql("UPDATE st_user_portfolio SET sort_order = :o WHERE stock_code = :c",
                      {"o": i, "c": c})
            _exec_sql("UPDATE sm_stock_snapshot SET sort_order = :o WHERE stock_code = :c",
                      {"o": i, "c": c})
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/portfolio/transact/{stock_code}")
def portfolio_transact(stock_code: str, body: PortfolioTransact):
    """加仓/减仓（东财算法：先写流水，再从流水全量重算成本价）"""
    try:
        _ensure_portfolio_position_columns()
        _ensure_portfolio_trans_log_table()
        code = stock_code.strip().zfill(6)
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
    except Exception:
        pass
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
    except Exception:
        pass
    try:
        rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_kline "
            "WHERE k_type = 1 AND trade_date <= :d",
            {"d": d_str},
        )
        if rows and rows[0].get("d"):
            return str(rows[0]["d"])[:10]
    except Exception:
        pass
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
    except Exception:
        pass
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
    except Exception:
        pass

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


def _portfolio_fetch_live_quotes(codes: list[str]) -> dict[str, dict]:
    """拉取最新价写入 sm_stock_current，返回 {stock_code: quote_dict}。"""
    import numpy as np

    clean = [str(c).strip().zfill(6) for c in codes if str(c).strip()]
    if not clean:
        return {}
    df = None
    try:
        from adata.stock.market.stock_market.stock_market import StockMarket
        df = StockMarket().list_market_current(code_list=clean)
    except Exception:
        df = None
    if df is None or df.empty:
        try:
            import requests

            symbols = ",".join([("sh" + c if c.startswith("6") else "sz" + c) for c in clean])
            resp = requests.get(
                f"https://hq.sinajs.cn/list={symbols}",
                headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn"},
                timeout=15,
            )
            rows = []
            for line in resp.text.strip().split("\n"):
                if "=" not in line or '""' in line:
                    continue
                var_part, val_part = line.split("=", 1)
                code = var_part.split("_")[-1][2:]
                fields = val_part.strip('";\r ').split(",")
                if len(fields) < 4:
                    continue
                try:
                    short_name = fields[0]
                    pre_close = float(fields[2] or 0)
                    price = float(fields[3] or 0)
                    if price <= 0:
                        price = pre_close
                    change = price - pre_close if pre_close > 0 else None
                    change_pct = change / pre_close * 100 if pre_close > 0 and change is not None else None
                    rows.append({
                        "stock_code": code,
                        "short_name": short_name,
                        "price": price,
                        "change": change,
                        "change_pct": change_pct,
                        "volume": float(fields[8] or 0) if len(fields) > 8 else None,
                        "amount": float(fields[9] or 0) if len(fields) > 9 else None,
                        "_quote_source": "sina",
                    })
                except Exception:
                    continue
            df = pd.DataFrame(rows)
        except Exception:
            df = None
    if df is None or df.empty:
        return {}

    now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    out: dict[str, dict] = {}
    for _, row in df.iterrows():
        sc = str(row.get("stock_code", "")).strip().zfill(6)
        if not sc:
            continue
        sn = str(row.get("short_name", ""))[:128]
        try:
            price = float(row.get("price", np.nan))
        except (ValueError, TypeError):
            price = None
        try:
            change = float(row.get("change", np.nan))
        except (ValueError, TypeError):
            change = None
        try:
            chg_pct = float(row.get("change_pct", np.nan))
        except (ValueError, TypeError):
            chg_pct = None
        try:
            volume = float(row.get("volume", np.nan))
        except (ValueError, TypeError):
            volume = None
        try:
            amount = float(row.get("amount", np.nan))
        except (ValueError, TypeError):
            amount = None
        _exec_sql("DELETE FROM sm_stock_current WHERE stock_code = :c", {"c": sc})
        _exec_sql("""
            INSERT INTO sm_stock_current (stock_code, short_name, price, `change`, change_pct, volume, amount, snapshot_at, etl_sync_at)
            VALUES (:c, :n, :p, :ch, :cp, :v, :a, :sn, :et)
        """, {"c": sc, "n": sn, "p": price, "ch": change, "cp": chg_pct, "v": volume, "a": amount, "sn": now_str, "et": now_str})
        out[sc] = {
            "price": price,
            "change": change,
            "change_pct": chg_pct,
            "short_name": sn,
            "snapshot_at": now_str,
            "source": str(row.get("_quote_source") or "live"),
        }
    return out


@router.get("/portfolio/live")
def portfolio_live():
    """自选股实时行情：拉取最新价 + 返回自选股数据（含当日盈亏/持仓盈亏），一次搞定"""
    try:
        _ensure_portfolio_position_columns()
        pf_rows = _read_sql("SELECT stock_code, short_name, cost_price, shares, last_operation, is_holding FROM st_user_portfolio ORDER BY (shares > 0) DESC, sort_order")
        codes = [r["stock_code"] for r in pf_rows] if pf_rows else []
        if not codes:
            return {"data": [], "total": 0}
        quotes = _portfolio_fetch_live_quotes(codes)
        if not quotes:
            quotes = {}
        _today_str = date.today().isoformat()
        result = []
        total_hold_profit = 0.0
        today_hold_profit = 0.0
        holding_count = 0
        today_open_count = 0
        today_cleared_count = 0
        for r in (pf_rows or []):
            sc = r["stock_code"]
            q = quotes.get(sc, {})
            cur_price = q.get("price") or 0
            chg_pct = q.get("change_pct") or 0
            cp = float(r.get("cost_price") or 0)
            sh = int(r.get("shares") or 0)
            profit = round((cur_price - cp) * sh, 2) if cur_price and cp else 0
            profit_pct = round((cur_price / cp - 1) * 100, 2) if cur_price and cp else 0
            trades = _portfolio_effective_today_trades(sc, sh, cp, r, _today_str)
            trade_state = _portfolio_today_trade_state(sh, trades)
            today_status = trade_state.get("today_position_status", "")
            today_profit = _portfolio_day_profit(
                sh,
                cur_price,
                cur_price,
                q.get("change"),
                None,
                chg_pct,
                sc,
                trades,
                _today_str,
            )
            if trade_state.get("is_today_open") or trade_state.get("is_today_reopened"):
                today_open_count += 1
            if trade_state.get("is_today_cleared"):
                today_cleared_count += 1
            if sh > 0:
                holding_count += 1
                total_hold_profit += profit
            if today_profit is not None:
                today_hold_profit += today_profit
            result.append({
                "stock_code": sc,
                "display_name": q.get("short_name") or r.get("short_name") or sc,
                "cur_price": cur_price,
                "change_pct": chg_pct,
                "cost_price": cp,
                "shares": sh,
                "profit": profit,
                "profit_pct": profit_pct,
                "today_profit": today_profit,
                "is_holding": sh > 0,
                "is_today_cleared": trade_state.get("is_today_cleared", False),
                "is_today_open": trade_state.get("is_today_open", False),
                "is_today_reopened": trade_state.get("is_today_reopened", False),
                "has_today_trade": trade_state.get("has_today_trade", False),
                "today_position_status": today_status,
                "snapshot_at": q.get("snapshot_at", ""),
            })
        return {
            "data": result,
            "total": len(result),
            "live": True,
            "summary": {
                "holding_count": holding_count,
                "total_hold_profit": round(total_hold_profit, 2),
                "today_hold_profit": round(today_hold_profit, 2),
                "today_open_count": today_open_count,
                "today_cleared_count": today_cleared_count,
            },
        }
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)[:200]}


@router.post("/portfolio/refresh-prices")
def portfolio_refresh_prices():
    """刷新自选股实时行情：拉取最新价写入sm_stock_current"""
    try:
        pf_rows = _read_sql("SELECT stock_code FROM st_user_portfolio ORDER BY sort_order")
        codes = [r["stock_code"] for r in pf_rows] if pf_rows else []
        if not codes:
            return {"status": "ok", "refreshed": 0, "message": "无自选股"}
        quotes = _portfolio_fetch_live_quotes(codes)
        if not quotes:
            return {"status": "error", "error": "行情接口无数据"}
        return {"status": "ok", "refreshed": len(quotes)}
    except ImportError:
        return {"status": "error", "error": "adata 模块不可用，无法获取实时行情"}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200]}


@router.get("/portfolio/analyze/{stock_code}")
def portfolio_analyze(stock_code: str):
    """AI 分析个股 — DeepSeek 生成详细文字分析"""
    try:
        code = stock_code.strip().zfill(6)

        # 获取交易日期
        td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = td_rows[0]["d"] if td_rows and td_rows[0].get("d") else date.today().isoformat()
        mode = _portfolio_market_mode()

        # 基本信息
        basic_rows = _read_sql(
            "SELECT stock_code, short_name, exchange, list_date FROM si_all_code WHERE stock_code = :c",
            {"c": code}
        )
        if not basic_rows:
            return {"error": f"股票 {code} 不存在"}
        basic = basic_rows[0]

        # 行业
        industry_rows = _read_sql(
            "SELECT plate_name FROM si_stock_plate_east WHERE stock_code = :c AND plate_type = '行业'",
            {"c": code}
        )
        industry = industry_rows[0]["plate_name"] if industry_rows else ""

        # 概念
        concept_rows = _read_sql(
            "SELECT DISTINCT name FROM si_stock_concept_east WHERE stock_code = :c LIMIT 20",
            {"c": code}
        )
        concepts = [r["name"] for r in concept_rows if r.get("name")]

        # 行情数据：盘中取实时，盘后取最新K线
        quote = {}
        if mode == "intraday":
            cur_rows = _read_sql(
                "SELECT price, change_pct, snapshot_at FROM sm_stock_current WHERE stock_code = :c",
                {"c": code}
            )
            if cur_rows and cur_rows[0].get("price") is not None:
                quote = {**cur_rows[0], "source": "realtime"}
        if not quote:
            kline_rows = _read_sql(
                "SELECT close AS price, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close, trade_date "
                "FROM sm_stock_kline WHERE stock_code = :c AND k_type=1 ORDER BY trade_date DESC LIMIT 1",
                {"c": code}
            )
            if kline_rows:
                quote = {**kline_rows[0], "source": "kline"}
                trade_date = kline_rows[0].get("trade_date", trade_date)

        price_val = float(quote.get("price") or 0)

        # 资本
        cap_rows = _read_sql(
            "SELECT total_shares, limit_shares, list_a_shares FROM si_stock_shares WHERE stock_code = :c",
            {"c": code}
        )
        cap = cap_rows[0] if cap_rows else {}

        # 量比
        vol_rows = _read_sql("""
            SELECT AVG(volume) AS avg_vol FROM (
                SELECT volume FROM sm_stock_kline
                WHERE stock_code = :c AND k_type=1 AND trade_date < :td
                ORDER BY trade_date DESC LIMIT 5
            ) t
        """, {"c": code, "td": trade_date})
        avg_vol = float(vol_rows[0]["avg_vol"]) if vol_rows and vol_rows[0].get("avg_vol") else 0
        cur_vol = float(quote.get("volume") or 0)
        volume_ratio = round(cur_vol / avg_vol, 2) if avg_vol > 0 else None

        # 财务
        fin_rows = _read_sql("""
            SELECT basic_eps, net_asset_ps, roe_wtd, roa_wtd, gross_margin, net_margin,
                   total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr, report_date
            FROM si_stock_finance WHERE stock_code = :c
            ORDER BY report_date DESC LIMIT 1
        """, {"c": code})
        fin = fin_rows[0] if fin_rows else {}
        eps = float(fin.get("basic_eps") or 0)
        bvps = float(fin.get("net_asset_ps") or 0)
        pe_ttm = round(price_val / eps, 2) if eps > 0 else None
        pb = round(price_val / bvps, 2) if bvps > 0 else None

        market = {
            "price": price_val,
            "change_pct": quote.get("change_pct"),
            "open": quote.get("open"),
            "high": quote.get("high"),
            "low": quote.get("low"),
            "volume": quote.get("volume"),
            "amount": quote.get("amount"),
            "turnover_ratio": quote.get("turnover_ratio"),
            "volume_ratio": volume_ratio,
            "pe_ttm": pe_ttm,
            "pb": pb,
            "total_market_cap": round(price_val * float(cap.get("total_shares") or 0) / 1e8, 2) if cap.get("total_shares") else None,
            "float_market_cap": round(price_val * float(cap.get("list_a_shares") or 0) / 1e8, 2) if cap.get("list_a_shares") else None,
        }

        # 资金流
        flow_td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily")
        flow_td = flow_td_rows[0]["d"] if flow_td_rows and flow_td_rows[0].get("d") else trade_date

        flow_rows = _read_sql(
            "SELECT main_net_inflow, max_net_inflow, lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source "
            "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :ftd",
            {"c": code, "ftd": flow_td}
        )
        flow_today = flow_rows[0] if flow_rows else {}

        flow_multi = {}
        for label, days in [("flow_3d", 3), ("flow_5d", 5), ("flow_20d", 20)]:
            mf = _read_sql(f"""
                SELECT SUM(main_net_inflow) AS main_net_inflow
                FROM sm_stock_capital_flow_daily
                WHERE stock_code = :c AND trade_date >= (
                    SELECT trade_date FROM sm_stock_kline WHERE k_type=1 AND trade_date <= :td
                    GROUP BY trade_date ORDER BY trade_date DESC LIMIT 1 OFFSET {days - 1}
                )
            """, {"c": code, "td": flow_td})
            flow_multi[label] = mf[0]["main_net_inflow"] if mf and mf[0].get("main_net_inflow") else None

        lhb_rows = _read_sql("""
            SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy
            FROM st_a_list_daily
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
        """, {"c": code, "td": trade_date})
        lhb = lhb_rows[0] if lhb_rows else {}

        lhb_seats = _read_sql("""
            SELECT trade_date, operate_name, a_net_amount, a_buy_amount, a_sell_amount
            FROM st_a_list_info
            WHERE stock_code = :c AND trade_date >= DATE_SUB(:td, INTERVAL 20 DAY)
            ORDER BY trade_date DESC LIMIT 10
        """, {"c": code, "td": trade_date})

        capital = {
            "today": flow_today,
            "flow_3d": flow_multi.get("flow_3d"),
            "flow_5d": flow_multi.get("flow_5d"),
            "flow_20d": flow_multi.get("flow_20d"),
            "dragon_tiger": {
                "count_20d": int(lhb.get("cnt") or 0),
                "inst_net_buy": lhb.get("inst_net_buy"),
                "seats": lhb_seats,
            },
        }

        # 财务面
        fin_detail = _read_sql("""
            SELECT report_date, report_type, basic_eps, net_asset_ps,
                   total_rev, net_profit_attr_sh, total_rev_yoy_gr, net_profit_yoy_gr,
                   roe_wtd, roa_wtd, gross_margin, net_margin,
                   curr_ratio, quick_ratio, asset_liab_ratio
            FROM si_stock_finance WHERE stock_code = :c
            ORDER BY report_date DESC LIMIT 8
        """, {"c": code})

        finance = {
            "latest": fin or {},
            "quarters": fin_detail,
        }

        # 估值面
        pe_history = _read_sql("""
            SELECT f.report_date, f.basic_eps, f.net_asset_ps
            FROM si_stock_finance f
            WHERE f.stock_code = :c AND f.basic_eps > 0
            ORDER BY f.report_date DESC LIMIT 20
        """, {"c": code})

        pe_percentile = None
        pb_percentile = None
        if pe_history and pe_ttm:
            pe_list = []
            for r in pe_history:
                e = float(r.get("basic_eps") or 0)
                if e > 0:
                    pe_list.append(price_val / e)
            if pe_list:
                below = sum(1 for p in pe_list if p <= pe_ttm)
                pe_percentile = round(below / len(pe_list) * 100, 1)

        if pe_history and pb:
            pb_list = []
            for r in pe_history:
                b = float(r.get("net_asset_ps") or 0)
                if b > 0:
                    pb_list.append(price_val / b)
            if pb_list:
                below = sum(1 for p in pb_list if p <= pb)
                pb_percentile = round(below / len(pb_list) * 100, 1)

        valuation = {
            "pe_ttm": pe_ttm,
            "pe_percentile": pe_percentile,
            "pb": pb,
            "pb_percentile": pb_percentile,
            "verdict": "偏高" if (pe_percentile and pe_percentile > 70) else "偏低" if (pe_percentile and pe_percentile < 30) else "合理" if pe_percentile else None,
        }

        # 技术面
        kline_250 = _read_sql("""
            SELECT trade_date, open, close, high, low, volume, change_pct
            FROM sm_stock_kline WHERE stock_code = :c AND k_type=1
            ORDER BY trade_date DESC LIMIT 260
        """, {"c": code})

        technical = _compute_technical(kline_250, price_val)

        # 持仓信息
        holding_rows = _read_sql(
            "SELECT shares, cost_price FROM st_user_portfolio WHERE stock_code = :c AND shares > 0",
            {"c": code}
        )
        holding = holding_rows[0] if holding_rows else None

        # DeepSeek AI 分析
        ai_result = _generate_ai_analysis(code, basic.get("short_name"), market, capital, finance, valuation, technical, industry, concepts, holding)

        # 构建返回
        analysis_text = ai_result.get("conclusion", "") if isinstance(ai_result, dict) else str(ai_result)
        data_mode_label = "盘中实时" if mode == "intraday" else "盘后收盘"

        response = {
            "stock_code": code,
            "short_name": basic.get("short_name"),
            "analysis": analysis_text,
            "data_mode": mode,
            "data_mode_label": data_mode_label,
            "quote_trade_date": str(trade_date),
            "ai_scores": ai_result.get("scores") if isinstance(ai_result, dict) else None,
            "ai_score": ai_result.get("score") if isinstance(ai_result, dict) else None,
            "ai_action": ai_result.get("action") if isinstance(ai_result, dict) else None,
            "ai_action_reason": ai_result.get("action_reason") if isinstance(ai_result, dict) else None,
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
        except Exception:
            pass

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
        except Exception:
            pass

        try:
            plate1 = _read_sql(
                "SELECT snapshot_date, concept_name, hot_value, plate_type FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date >= :b AND snapshot_date <= :e AND plate_type = 1 "
                "ORDER BY snapshot_date DESC",
                {"b": begin.isoformat(), "e": end_dt.isoformat()},
            )
            all_plate1_rows.extend(plate1)
        except Exception:
            pass

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
        except Exception:
            pass

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

        return {"date": end_date, "dates": [r["date"] for r in data],
                "groups": groups, "data": data, "total": len(data), "east_tree": east_tree,
                "daily_totals": daily_totals, "raw_data": raw_data}
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
                    "INSERT INTO st_hot_concept_ths_daily (snapshot_date,plate_type,rank,concept_code,concept_name,change_pct,hot_value,hot_tag,etl_sync_at) "
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
    child_env = _os.environ.copy()
    child_env.setdefault("MYSQL_URL", _os.environ.get("MYSQL_URL", "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"))
    child_env.setdefault("PYTHONPATH", _ROOT + ":" + _os.path.join(_ROOT, "adata"))
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
):
    """市场情绪与风格分析 — 主线/轮动/大小盘"""
    # 盘中 120s 缓存，盘后 300s 缓存（情绪分析计算量大，缓存更长）
    _ttl = 120 if _is_monitor_trading_time() else 300
    _ckey = f"market_sentiment_{date}_{days}_{top}"
    cached = _cache_get(_ckey, ttl_seconds=_ttl)
    if cached is not None:
        return cached
    try:
        from tools.market_sentiment import run_full_analysis
        result = run_full_analysis(lookback_days=days, end_date=date, top_n=top, engine=get_engine())
        if "error" not in result:
            _cache_set(_ckey, result)
        return result
    except Exception as e:
        return {"error": str(e)}


def _exec_sql(sql: str, params: dict = None):
    from server.api.routers._engine import get_engine as _ge
    e = _ge()
    with e.begin() as c:
        c.execute(text(sql), params)


def _read_dotenv_key():
    for p in [_Path(__file__).resolve().parents[3] / ".env", _Path("/opt/ProBigA/.env")]:
        if p.is_file():
            for line in open(p):
                line = line.strip()
                if line.startswith("DEEPSEEK_API_KEY="):
                    return line.split("=", 1)[1].strip()
    return ""


def _is_monitor_trading_time():
    from datetime import datetime as _dt
    now = _dt.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    return (925 <= t <= 1135) or (1255 <= t <= 1505)


def _get_realtime_overview():
    try:
        snap = _read_sql(
            "SELECT MAX(snapshot_at) AS sa FROM sm_rt_quote_snapshot "
            "WHERE snapshot_at >= NOW() - INTERVAL 10 MINUTE"
        )
        if not snap or not snap[0].get("sa"):
            return None
        sa = snap[0]["sa"]
        rows = _read_sql(
            "SELECT "
            "  SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt, "
            "  SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt, "
            "  SUM(CASE WHEN ABS(change_pct) < 1 THEN 1 ELSE 0 END) AS sideline_cnt, "
            "  COUNT(*) AS total, "
            "  COALESCE(SUM(amount), 0) AS total_amount "
            "FROM sm_rt_quote_snapshot WHERE snapshot_at = :sa",
            {"sa": sa},
        )
        if rows and rows[0].get("total") and int(rows[0]["total"]) > 100:
            return rows[0]
    except Exception:
        pass
    return None


@router.get("/sector/movement")
def sector_movement(group_by: str = Query(default="industry", regex="^(industry|concept|all)$")):
    """板块异动检测 + 龙头识别

    group_by: industry=按行业分组(申万一级), concept=按概念分组, all=同时展示两组
    """
    _ttl = 30 if _is_monitor_trading_time() else 120
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

        if has_prev:
            raw = _read_sql("""
                SELECT
                    n.stock_code,
                    n.short_name,
                    n.change_pct AS now_pct,
                    n.amount AS now_amt,
                    p.change_pct AS prev_pct
                FROM sm_rt_quote_snapshot n
                JOIN sm_rt_quote_snapshot p
                    ON p.stock_code = n.stock_code AND p.snapshot_at = :psa
                WHERE n.snapshot_at = :nsa
            """, {"nsa": now_sa, "psa": prev_sa})
        else:
            raw = _read_sql("""
                SELECT stock_code, short_name, change_pct AS now_pct, amount AS now_amt, NULL AS prev_pct
                FROM sm_rt_quote_snapshot WHERE snapshot_at = :nsa
            """, {"nsa": now_sa})

        codes = [str(r["stock_code"]) for r in raw]
        if not codes:
            return {"sectors": []}

        ph = ",".join([f"'{c}'" for c in codes])

        # 获取概念板块映射
        concept_map = defaultdict(set)
        if group_by in ("concept", "all"):
            try:
                concept_rows = _read_sql(f"""
                    SELECT stock_code, name AS plate_name
                    FROM si_stock_concept_map
                    WHERE stock_code IN ({ph})
                """)
                for sr in concept_rows:
                    concept_map[str(sr["stock_code"])].add(str(sr["plate_name"]))
            except Exception:
                pass

        # 获取行业板块映射（申万一级行业）
        industry_map = defaultdict(set)
        if group_by in ("industry", "all"):
            try:
                industry_rows = _read_sql(f"""
                    SELECT stock_code, industry_name
                    FROM si_industry_sw
                    WHERE stock_code IN ({ph}) AND industry_type = '申万一级'
                """)
                for sr in industry_rows:
                    name = str(sr["industry_name"] or "").strip()
                    if name:
                        industry_map[str(sr["stock_code"])].add(name)
            except Exception:
                pass

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
    # 盘中 30s 缓存，盘后 120s 缓存
    _ttl = 30 if _is_monitor_trading_time() else 120
    _ckey = f"monitor_data_{date}"
    cached = _cache_get(_ckey, ttl_seconds=_ttl)
    if cached is not None:
        return cached
    try:
        latest_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type = 1")
        if not latest_rows or not latest_rows[0].get("d"):
            return {"error": "无交易数据"}
        trade_date = str(latest_rows[0]["d"])[:10]

        is_realtime = False
        rt_overview = _get_realtime_overview() if _is_monitor_trading_time() else None

        if rt_overview:
            cur = rt_overview
            is_realtime = True
        else:
            overview_sql = """
            SELECT
                SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
                SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt,
                SUM(CASE WHEN ABS(change_pct) < 1 THEN 1 ELSE 0 END) AS sideline_cnt,
                COUNT(*) AS total,
                COALESCE(SUM(amount), 0) AS total_amount
            FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
            """
            cur_rows = _read_sql(overview_sql, {"d": trade_date})
            if not cur_rows or cur_rows[0]["total"] == 0:
                return {"error": f"交易日 {trade_date} 无数据"}
            cur = cur_rows[0]

        up_cnt = int(cur["up_cnt"] or 0)
        total = int(cur["total"] or 1)
        total_amount = float(cur["total_amount"] or 0)
        sideline_ratio = round(int(cur["sideline_cnt"] or 0) / total * 100, 2)
        market_heat = round(up_cnt / total * 1000, 0)

        prev_date_rows = _read_sql(
            "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type = 1 AND trade_date < :d",
            {"d": trade_date},
        )
        prev_date = str(prev_date_rows[0]["d"])[:10] if prev_date_rows and prev_date_rows[0].get("d") else None

        prev_heat = 0
        if prev_date:
            prev_overview = _read_sql(overview_sql, {"d": prev_date})
            if prev_overview and prev_overview[0].get("total"):
                prev_up = int(prev_overview[0]["up_cnt"] or 0)
                prev_total = int(prev_overview[0]["total"] or 1)
                prev_heat = round(prev_up / prev_total * 1000, 0)

        heat_change = round(((market_heat - prev_heat) / prev_heat * 100) if prev_heat > 0 else 0, 2)

        # 一次性查询所有板块类型和日期的数据（替代4次顺序查询+回退）
        _query_dates = [trade_date] + ([prev_date] if prev_date else [])
        _ph_dates = ",".join([f"'{d}'" for d in _query_dates])
        all_hot_rows = _read_sql(
            f"SELECT snapshot_date, plate_type, concept_name, hot_value, change_pct "
            f"FROM st_hot_concept_ths_daily "
            f"WHERE snapshot_date IN ({_ph_dates}) AND plate_type IN (1, 2, 3) "
            f"ORDER BY plate_type, snapshot_date, hot_value DESC"
        )

        # 按优先级提取行业数据：plate_type=3（东财）> plate_type=2（THS），先当日再前日
        industry_rows = []
        for _pt in (3, 2):
            for _qd in _query_dates:
                _filtered = [r for r in all_hot_rows if r["plate_type"] == _pt and str(r["snapshot_date"])[:10] == _qd]
                if _filtered:
                    industry_rows = _filtered[:10]
                    break
            if industry_rows:
                break

        # 概念数据：plate_type=1，先当日再前日
        concept_rows = []
        for _qd in _query_dates:
            _filtered = [r for r in all_hot_rows if r["plate_type"] == 1 and str(r["snapshot_date"])[:10] == _qd]
            if _filtered:
                concept_rows = _filtered[:10]
                break

        TMT_CHILDREN = (
            "电子化学品", "半导体", "消费电子", "其他电子", "光学光电子", "元件",
            "软件开发", "IT服务", "计算机设备",
            "通信服务", "通信设备",
            "广告营销", "影视院线", "数字媒体", "游戏", "出版", "电视广播",
        )
        tmt_ratio = 0
        if industry_rows:
            tmt_hot = sum(float(r["hot_value"] or 0) for r in industry_rows if r.get("concept_name") in TMT_CHILDREN)
            total_hot = sum(float(r["hot_value"] or 0) for r in industry_rows)
            if total_hot > 0:
                tmt_ratio = round(tmt_hot / total_hot * 100, 2)

        csi1000_rows = _read_sql(
            "SELECT close AS price, change_pct FROM sm_index_kline "
            "WHERE trade_date = :d AND index_code = '000852' AND k_type = 1",
            {"d": trade_date},
        )
        csi1000_price = float(csi1000_rows[0]["price"] or 0) if csi1000_rows else 0
        csi1000_chg = float(csi1000_rows[0]["change_pct"] or 0) if csi1000_rows else 0

        if csi1000_price == 0:
            small_cap_sql = """
            SELECT
                SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
                COUNT(*) AS total,
                AVG(change_pct) AS avg_chg
            FROM sm_stock_kline
            WHERE trade_date = :d AND k_type = 1
              AND (stock_code LIKE '002%%' OR stock_code LIKE '300%%' OR stock_code LIKE '301%%')
            """
            sc_rows = _read_sql(small_cap_sql, {"d": trade_date})
            if sc_rows and sc_rows[0].get("total") and int(sc_rows[0]["total"] or 0) > 0:
                sc = sc_rows[0]
                sc_total = int(sc["total"] or 1)
                csi1000_price = round(int(sc["up_cnt"] or 0) / sc_total * 1000, 0)
                csi1000_chg = round(float(sc["avg_chg"] or 0), 2)

        history_dates = []
        history_heat = []
        history_amount = []
        history_sideline = []
        history_tmt = []
        history_csi1000_heat = []

        # 一次性 GROUP BY 查询 20 个交易日的历史数据（加日期范围避免全表扫描）
        _hist_start = str(trade_date)[:8] + "01"  # 月初起始，覆盖足够天数
        history_batch_sql = """
        SELECT trade_date,
            SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
            COUNT(*) AS total,
            COALESCE(SUM(amount), 0) AS amt,
            SUM(CASE WHEN ABS(change_pct) < 1 THEN 1 ELSE 0 END) AS sideline_cnt
        FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date >= :hist_start
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT 20
        """
        history_batch = _read_sql(history_batch_sql, {"hist_start": _hist_start})
        # 按日期正序排列（从旧到新）
        history_batch = list(reversed(history_batch))
        for dr in history_batch:
            d = str(dr["trade_date"])[:10]
            history_dates.append(d[-5:])
            day_total = int(dr.get("total") or 0)
            if day_total > 0:
                h = round(int(dr["up_cnt"] or 0) / day_total * 1000, 0)
                history_heat.append(h)
                history_amount.append(round(float(dr["amt"] or 0) / 1e8, 0))
                history_sideline.append(round(int(dr["sideline_cnt"] or 0) / day_total * 100, 2))
                history_tmt.append(round(25 + h * 0.045, 1))
                history_csi1000_heat.append(round(400 + h * 0.6, 0))
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
                })

        signal = "低位徘徊" if market_heat < 400 else "高位运行" if market_heat > 600 else "震荡整理"

        _result = {
            "trade_date": trade_date,
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "is_realtime": is_realtime,
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
                "heat": round(market_heat * 0.9, 0),
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
                "style_judge": f"小盘热度{csi1000_price:.0f}，{'偏弱' if csi1000_chg < 0 else '偏强'}，平均涨跌{csi1000_chg:+.2f}%",
                "capital_flow": f"观望资金占比{sideline_ratio:.2f}%，TMT合计占比{tmt_ratio:.2f}%",
                "signal": f"全A{signal}，关注{'周期股补涨机会' if market_heat < 400 else '科技板块轮动'}",
            },
        }
        _cache_set(_ckey, _result)
        return _result
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


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
                        INSERT INTO jq_strategy_picks (strategy_name, stock_code, short_name, score, reason, pick_date)
                        VALUES (:s, :c, :n, :sc, :r, :d)
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
                    INSERT INTO jq_strategy_meta (strategy_name, description, last_run_date)
                    VALUES (:s, :desc, :d)
                    ON DUPLICATE KEY UPDATE last_run_date = :d, description = IF(:desc != '', :desc, description)
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
    try:
        from biz.stock_market.realtime_quotes import fetch_list_market_current, save_to_mysql
        from sqlalchemy import text
        
        engine = get_engine()
        
        codes_sql = "SELECT DISTINCT stock_code FROM si_all_code WHERE stock_code REGEXP '^[0-9]{6}$'"
        codes_rows = _read_sql(codes_sql)
        if not codes_rows:
            return {"error": "无股票代码", "synced": 0}
        
        all_codes = [str(r["stock_code"]).zfill(6) for r in codes_rows]
        
        batch_size = 500
        total_synced = 0
        
        for i in range(0, len(all_codes), batch_size):
            batch = all_codes[i:i+batch_size]
            try:
                df = fetch_list_market_current(batch)
                if not df.empty:
                    ts = datetime.now().replace(microsecond=0)
                    df["snapshot_at"] = ts
                    df.to_sql(
                        "sm_rt_quote_snapshot",
                        engine,
                        if_exists="append",
                        index=False,
                        chunksize=500,
                        method="multi"
                    )
                    total_synced += len(df)
            except Exception as e:
                continue
        
        return {
            "success": True,
            "synced": total_synced,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


# ═══════════════════════════════════════════
# AI 推荐买入股票
# ═══════════════════════════════════════════

@router.get("/hot-data/recommended-stocks")
def recommended_stocks(trade_date: str = Query(default="")):
    """获取 AI 推荐买入股票列表"""
    try:
        if not trade_date:
            trade_date = _latest_date("st_recommended_stocks", "pick_date")
        rows = _read_sql("""
            SELECT r.stock_code, r.short_name, r.ai_score, r.fundamental,
                   r.capital_score, r.valuation, r.technical, r.reason,
                   r.sources, r.pick_date,
                   r.long_term_score, r.short_term_score,
                   r.recommend_status, r.recommend_reason, r.event_risk_level,
                   r.sentiment_score, r.market_mood_score
            FROM st_recommended_stocks r
            WHERE r.pick_date = :d
            ORDER BY r.ai_score DESC
        """, {"d": trade_date})
        # 如果当天没数据，自动回退到最近有数据的日期
        if not rows:
            fallback = _read_sql("SELECT MAX(pick_date) AS d FROM st_recommended_stocks", {})
            if fallback and fallback[0].get("d"):
                trade_date = str(fallback[0]["d"])
                rows = _read_sql("""
                    SELECT r.stock_code, r.short_name, r.ai_score, r.fundamental,
                           r.capital_score, r.valuation, r.technical, r.reason,
                           r.sources, r.pick_date,
                           r.long_term_score, r.short_term_score,
                           r.recommend_status, r.recommend_reason, r.event_risk_level,
                           r.sentiment_score, r.market_mood_score
                    FROM st_recommended_stocks r
                    WHERE r.pick_date = :d
                    ORDER BY r.ai_score DESC
                """, {"d": trade_date})
        if not rows:
            return {"date": trade_date, "data": [], "total": 0, "note": "暂无推荐数据，请先运行筛选"}
        # 关联最新行情
        codes = [r["stock_code"] for r in rows]
        placeholders = ",".join(f"'{c}'" for c in codes)
        quotes = {}
        try:
            latest_date = _latest_date("sm_stock_kline")
            q_rows = _read_sql(f"""
                SELECT stock_code, close AS price, change_pct, amount
                FROM sm_stock_kline
                WHERE stock_code IN ({placeholders}) AND k_type=1
                  AND trade_date = :d
            """, {"d": latest_date})
            for q in q_rows:
                quotes[q["stock_code"]] = q
        except Exception:
            pass
        for r in rows:
            q = quotes.get(r["stock_code"], {})
            r["price"] = q.get("price")
            r["change_pct"] = q.get("change_pct")
            r["amount"] = q.get("amount")
        return {"date": trade_date, "data": rows, "total": len(rows)}
    except Exception as e:
        return {"date": trade_date, "data": [], "total": 0, "error": str(e)}


def _smart_trade_date() -> str:
    """智能判断当前应该使用的交易日期：
    - 盘中（工作日 9:30-15:00）：使用今天日期
    - 盘后/周末：使用最新已有数据的交易日
    """
    from datetime import datetime, timedelta
    import pytz
    tz = pytz.timezone("Asia/Shanghai")
    now = datetime.now(tz)
    weekday = now.weekday()  # 0=周一, 6=周日
    hour_min = now.hour * 60 + now.minute
    # 盘中判断：工作日 9:30(570) - 15:00(900)
    is_trading = weekday < 5 and 570 <= hour_min <= 900
    if is_trading:
        return now.strftime("%Y-%m-%d")
    return _latest_date("sm_stock_kline")


@router.post("/hot-data/recommended-stocks/run")
def run_recommended_stocks(trade_date: str = Query(default=""), min_score: int = Query(default=50)):
    """触发 AI 推荐股票筛选（使用统一分析引擎）"""
    import threading

    if not trade_date:
        trade_date = _smart_trade_date()

    def _run_screen():
        try:
            from tools.screen_stocks import run_trend_strong, run_low_start, run_trend, run_flow

            engine = get_engine()
            top_per_mode = 30

            # 量化初选
            all_dfs = []
            screeners = [
                ("trend_strong", lambda: run_trend_strong(engine, trade_date, top_per_mode, 1, 0, 10, 0.5, 0.8, 2.5, 150.0, 0.95)),
                ("low_start", lambda: run_low_start(engine, trade_date, top_per_mode, 1, 0, 60, 0.28, 1.25, 2.0, 10.5)),
                ("trend", lambda: run_trend(engine, trade_date, top_per_mode, 1, 0, 0)),
                ("flow", lambda: run_flow(engine, trade_date, top_per_mode, 5_000_000)),
            ]
            for name, fn in screeners:
                try:
                    df = fn()
                    if df is not None and not df.empty:
                        df["_source"] = name
                        all_dfs.append(df)
                except Exception:
                    pass

            if not all_dfs:
                return

            combined = pd.concat(all_dfs, ignore_index=True)
            combined["stock_code"] = combined["stock_code"].astype(str).str.strip().str.zfill(6)
            if "short_name" in combined.columns:
                combined = combined[~combined["short_name"].fillna("").str.contains("ST", case=False)]
            combined = combined[combined["stock_code"].str.match(r"^(0|6)")]
            dedup = combined.drop_duplicates(subset=["stock_code"])
            sources = combined.groupby("stock_code")["_source"].apply(lambda x: "+".join(sorted(set(x)))).reset_index()
            sources.columns = ["stock_code", "sources"]
            dedup = dedup.merge(sources, on="stock_code", how="left")

            # 使用统一分析引擎进行评分
            from server.engine.stock_analysis_engine import StockAnalysisEngine
            analysis_engine = StockAnalysisEngine()

            results = []
            for _, row in dedup.iterrows():
                code = str(row["stock_code"]).zfill(6)
                name = row.get("short_name", code)

                try:
                    # 使用统一引擎分析
                    result = analysis_engine.analyze(code, full_data=True)

                    # 保留所有评分 >= min_score 的股票（不要求 ALLOW 状态）
                    if result.short_term_score >= min_score:
                        results.append({
                            "stock_code": code,
                            "short_name": name,
                            "ai_score": result.short_term_score,
                            "long_term_score": result.long_term_score,
                            "short_term_score": result.short_term_score,
                            "fundamental": result.scores.fundamental,
                            "capital_score": result.scores.capital,
                            "valuation": result.scores.valuation,
                            "technical": result.scores.technical,
                            "reason": result.summary,
                            "sources": row.get("sources", ""),
                            "pick_date": trade_date,
                            "recommend_status": result.recommend.status,
                            "recommend_reason": result.recommend.reason,
                            "event_risk_level": result.event_risk.level,
                            "sentiment_score": result.scores.sentiment,
                            "market_mood_score": result.scores.market_mood,
                        })
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"分析 {code} 失败: {e}")
                    continue

            # 按短线评分降序排序
            results.sort(key=lambda x: x.get("short_term_score", 0), reverse=True)

            # 写入数据库
            if results:
                with engine.begin() as conn:
                    # 确保新字段存在（兼容不支持 IF NOT EXISTS 的 MySQL 版本）
                    for col, dtype in [("sentiment_score", "FLOAT"), ("market_mood_score", "FLOAT")]:
                        try:
                            conn.execute(text(f"ALTER TABLE st_recommended_stocks ADD COLUMN {col} {dtype}"))
                        except Exception:
                            pass  # 列已存在（Duplicate column）

                    conn.execute(text("DELETE FROM st_recommended_stocks WHERE pick_date = :d"), {"d": trade_date})
                    for r in results:
                        conn.execute(text("""
                            INSERT INTO st_recommended_stocks
                            (stock_code, short_name, ai_score, long_term_score, short_term_score,
                             fundamental, capital_score, valuation, technical,
                             reason, sources, pick_date,
                             recommend_status, recommend_reason, event_risk_level,
                             sentiment_score, market_mood_score, created_at)
                            VALUES (:code, :name, :score, :lt_score, :st_score,
                                    :fund, :cap, :val, :tech,
                                    :reason, :sources, :pick,
                                    :rec_status, :rec_reason, :risk_level,
                                    :sentiment, :market_mood, NOW())
                        """), {
                            "code": r["stock_code"], "name": r["short_name"], "score": r["ai_score"],
                            "lt_score": r.get("long_term_score"), "st_score": r.get("short_term_score"),
                            "fund": r["fundamental"], "cap": r["capital_score"], "val": r["valuation"],
                            "tech": r["technical"], "reason": r["reason"], "sources": r["sources"],
                            "pick": r["pick_date"],
                            "rec_status": r.get("recommend_status", "ALLOW"),
                            "rec_reason": r.get("recommend_reason", ""),
                            "risk_level": r.get("event_risk_level", "LOW"),
                            "sentiment": r.get("sentiment_score"),
                            "market_mood": r.get("market_mood_score"),
                        })

                    import logging
                    logging.getLogger(__name__).info(f"推荐筛选完成，共 {len(results)} 只股票")
        except Exception as ex:
            import logging
            logging.getLogger(__name__).error(f"recommended-stocks error: {ex}", exc_info=True)

    t = threading.Thread(target=_run_screen, daemon=True)
    t.start()
    return {"status": "started", "date": trade_date, "min_score": min_score, "note": "筛选已启动，完成后刷新页面查看结果"}


# ──────────────────────────────────────────────────────────────────────
#  主力行为分析 (建仓 / 洗盘 / 出货)
# ──────────────────────────────────────────────────────────────────────

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
    return _compute_mainforce_behavior(stock_code, trade_date)


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
    if not trade_date:
        td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = str(td_rows[0]["d"]) if td_rows and td_rows[0].get("d") else str(date.today())

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
            analysis = _compute_mainforce_behavior(code, trade_date)
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
        except Exception:
            pass
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

@router.get("/hot-data/sector-rotation")
def sector_rotation(trade_date: str = Query(default=None), days: int = Query(default=10, ge=3, le=30)):
    """
    板块轮动分析：告诉你哪些板块在退潮、哪些在崛起，给出调仓换股建议。
    综合三个维度：热度排名变化 + 板块K线动量 + 板块资金流向。
    """
    if not trade_date:
        td_rows = _read_sql("SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
        trade_date = str(td_rows[0]["d"]) if td_rows and td_rows[0].get("d") else str(date.today())

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
            SELECT snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date >= :sd AND snapshot_date <= :td
              AND (plate_type = 3 OR plate_type = 1)
            ORDER BY snapshot_date, plate_type, rank
        """, {"sd": start_date, "td": trade_date})
        data_source = "east"
    else:
        # 回退到THS数据
        rank_rows = _read_sql("""
            SELECT snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date >= :sd AND snapshot_date <= :td AND plate_type IN (1, 2)
            ORDER BY snapshot_date, plate_type, rank
        """, {"sd": start_date, "td": trade_date})
        data_source = "ths"

    if not rank_rows:
        return {"trade_date": trade_date, "error": "暂无板块排名数据"}

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
        return {"trade_date": trade_date, "error": "数据天数不足"}

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

    return {
        "trade_date": trade_date,
        "data_source": data_source,
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


# ── 盘中实时数据刷新 ──
@router.post("/realtime/refresh")
def realtime_refresh(only: str = Query(default="all", regex="^(all|snapshot|flow|concept|index)$")):
    """手动刷新盘中数据（行情快照、资金流向、概念行情、指数行情）"""
    import subprocess as _sp
    script = str(_ROOT / "tools" / "crawl_realtime_batch.py")
    env = {**os.environ, "MYSQL_URL": str(get_engine().url)}
    try:
        result = _sp.run(
            [sys.executable, script, "--only", only],
            capture_output=True, text=True, timeout=120, env=env,
            cwd=str(_ROOT),
        )
        return {
            "success": result.returncode == 0,
            "output": result.stdout[-500:] if result.stdout else "",
            "error": result.stderr[-500:] if result.stderr else "",
        }
    except _sp.TimeoutExpired:
        return {"success": False, "error": "timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}

