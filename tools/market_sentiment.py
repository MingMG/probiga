#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场情绪与风格分析

分析维度:
  1. 主线识别 — 是否存在持续强势的领涨板块（主线）, 还是快速轮动
  2. 风格偏好 — 大盘股 vs 小盘股孰强, 成长 vs 价值风格

用法:
  python tools/market_sentiment.py                              # 最近20个交易日
  python tools/market_sentiment.py --date 2026-05-16            # 指定截止日期
  python tools/market_sentiment.py --days 10                    # 只看最近10日
  python tools/market_sentiment.py --top 5                      # 展示TOP N概念
  python tools/market_sentiment.py --json                       # JSON输出(供API使用)

环境变量:
  MYSQL_URL  MySQL连接
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from tools.env_config import create_tool_engine, resolve_tool_mysql_url

INDEX_MAP = {
    "000016": "上证50",
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "399303": "国证2000",
    "399006": "创业板指",
    "000688": "科创50",
}
MIN_THEME_ROTATION_DAYS = 2
MIN_THEME_DATE_COVERAGE_PCT = 80.0
MIN_THEME_DAILY_TOP10_COUNT = 8
MIN_MARKET_BREADTH_STOCK_COVERAGE_PCT = 80.0
MIN_CAPITAL_FLOW_STOCK_COVERAGE_PCT = 80.0


def _engine():
    return create_tool_engine()


def _to_date_str(val: any) -> str:
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)[:10]


def _normalize_date_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.copy()
    if col in df.columns:
        df["_datestr"] = df[col].apply(_to_date_str)
    return df


def _get_trade_dates(engine, end_date: str, lookback_days: int) -> list[str]:
    q = text("""
        SELECT DISTINCT trade_date FROM sm_stock_kline
        WHERE trade_date <= :end AND k_type = 1
        ORDER BY trade_date DESC
        LIMIT :n
    """)
    df = pd.read_sql(q, engine, params={"end": end_date, "n": lookback_days + 5})
    if df.empty:
        return []
    dates = sorted({_to_date_str(v) for v in df["trade_date"].values if v is not None})
    return dates[-lookback_days:] if len(dates) > lookback_days else dates


# ═══════════════════════════════════════════
# 1. 主线与轮动分析
# ═══════════════════════════════════════════

def analyze_main_theme(engine, trade_dates: list[str], top_n: int = 5) -> dict:
    """
    主线识别:
      - 统计各概念在最近N日TOP榜中的出现频率
      - 计算主线分数(连续出现+高排名)
      - 判断是主线行情还是轮动行情
    """
    if not trade_dates:
        return {"status": "no_data", "message": "无交易日数据"}

    d1 = trade_dates[0]
    d2 = trade_dates[-1]
    n_days = len(trade_dates)
    requested_dates = sorted({_to_date_str(value) for value in trade_dates})

    # 1.1 获取每日概念热度榜
    q = text("""
        SELECT snapshot_date, plate_type, rank, concept_code, concept_name,
               change_pct, hot_value, hot_tag
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date >= :d1 AND snapshot_date <= :d2
        ORDER BY snapshot_date, plate_type, rank
    """)
    df = pd.read_sql(q, engine, params={"d1": d1, "d2": d2})
    if df.empty:
        return {
            "status": "no_data",
            "message": "概念热度数据为空",
            "data_cutoff": None,
            "lookback_days": 0,
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "coverage": {
                "requested_trade_days": n_days,
                "available_concept_days": 0,
                "missing_trade_dates": requested_dates,
                "date_coverage_pct": 0.0,
                "minimum_date_coverage_pct": MIN_THEME_DATE_COVERAGE_PCT,
                "date_coverage_status": "incomplete",
                "minimum_rotation_days": MIN_THEME_ROTATION_DAYS,
                "minimum_daily_top10_count": MIN_THEME_DAILY_TOP10_COUNT,
                "daily_top10_status": "incomplete",
                "daily_top10_coverage": [],
                "requested_cutoff_covered": False,
            },
        }

    concept_df = df[df["plate_type"] == 1].copy()
    industry_df = df[df["plate_type"] == 2].copy()
    concept_dates = sorted(
        {
            _to_date_str(value)
            for value in concept_df.get("snapshot_date", pd.Series(dtype=object)).tolist()
            if value is not None and _to_date_str(value) in requested_dates
        }
    )
    concept_cutoff = concept_dates[-1] if concept_dates else None
    missing_concept_dates = sorted(set(requested_dates) - set(concept_dates))
    date_coverage_pct = len(concept_dates) / max(n_days, 1) * 100

    valid_top10 = _normalize_date_col(concept_df, "snapshot_date")
    valid_top10["_rank_numeric"] = pd.to_numeric(valid_top10["rank"], errors="coerce")
    valid_top10 = valid_top10[
        valid_top10["_datestr"].isin(requested_dates)
        & valid_top10["concept_name"].notna()
        & (valid_top10["concept_name"].astype(str).str.strip() != "")
        & np.isfinite(valid_top10["_rank_numeric"])
        & valid_top10["_rank_numeric"].between(1, 10)
    ]
    daily_top10_counts: dict[str, int] = {}
    for trade_date_value, day_rows in valid_top10.groupby("_datestr"):
        daily_top10_counts[str(trade_date_value)] = min(
            int(day_rows["concept_name"].nunique()),
            int(day_rows["_rank_numeric"].nunique()),
        )
    daily_top10_coverage = [
        {
            "trade_date": trade_date_value,
            "top10_item_count": daily_top10_counts.get(trade_date_value, 0),
        }
        for trade_date_value in requested_dates
    ]
    date_coverage_complete = date_coverage_pct >= MIN_THEME_DATE_COVERAGE_PCT
    available_daily_top10_complete = bool(concept_dates) and all(
        daily_top10_counts.get(trade_date_value, 0) >= MIN_THEME_DAILY_TOP10_COUNT
        for trade_date_value in concept_dates
    )
    concept_coverage = {
        "requested_trade_days": n_days,
        "available_concept_days": len(concept_dates),
        "missing_trade_dates": missing_concept_dates,
        "date_coverage_pct": round(date_coverage_pct, 1),
        "minimum_date_coverage_pct": MIN_THEME_DATE_COVERAGE_PCT,
        "date_coverage_status": "complete" if date_coverage_complete else "incomplete",
        "minimum_rotation_days": MIN_THEME_ROTATION_DAYS,
        "minimum_daily_top10_count": MIN_THEME_DAILY_TOP10_COUNT,
        "daily_top10_status": "complete" if available_daily_top10_complete else "incomplete",
        "daily_top10_coverage": daily_top10_coverage,
        "requested_cutoff_covered": concept_cutoff == d2,
    }

    # 1.2 概念 — 主线识别
    concept_stats = _calc_theme_stats(concept_df, trade_dates, "概念")
    industry_stats = _calc_theme_stats(industry_df, trade_dates, "行业")

    # 1.3 综合评估轮动强度
    combined = concept_df if not concept_df.empty else industry_df

    # 1.4 主线结论
    main_themes = []
    all_theme_stats = concept_stats.get("theme_scores", []) + industry_stats.get("theme_scores", [])
    all_theme_stats.sort(key=lambda x: x["score"], reverse=True)

    for t in all_theme_stats[:top_n]:
        main_themes.append({
            "name": t["name"],
            "type": t["type"],
            "code": t["code"],
            "appear_days": t["appear_days"],
            "avg_rank": round(t["avg_rank"], 1),
            "avg_change_pct": round(t["avg_change_pct"], 2),
            "score": round(t["score"], 1),
            "hot_value_total": round(t["hot_value_total"], 1),
            "recent_ranks": t["recent_ranks"],
        })

    # 结论
    if len(concept_dates) < MIN_THEME_ROTATION_DAYS:
        return {
            "status": "insufficient_history",
            "reason": "HOT_THEME_MINIMUM_HISTORY_MISSING",
            "data_cutoff": concept_cutoff,
            "lookback_days": len(concept_dates),
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "phase": None,
            "phase_desc": f"至少需要{MIN_THEME_ROTATION_DAYS}个实际概念榜交易日，才能判断热点是否轮动。",
            "rotation_score": None,
            "coverage": concept_coverage,
            "main_themes": main_themes,
            "concept_top_changes": concept_stats.get("rank_changes", []),
            "industry_top_changes": industry_stats.get("rank_changes", []),
        }
    if concept_cutoff != d2:
        return {
            "status": "partial",
            "reason": "HOT_THEME_REQUESTED_CUTOFF_MISSING",
            "data_cutoff": concept_cutoff,
            "lookback_days": len(concept_dates),
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "phase": None,
            "phase_desc": f"概念榜仅更新至{concept_cutoff or '-'}，未覆盖请求截止日{d2}，暂不判断轮动。",
            "rotation_score": None,
            "coverage": concept_coverage,
            "main_themes": main_themes,
            "concept_top_changes": concept_stats.get("rank_changes", []),
            "industry_top_changes": industry_stats.get("rank_changes", []),
        }
    if not date_coverage_complete:
        return {
            "status": "partial",
            "reason": "HOT_THEME_DATE_COVERAGE_INCOMPLETE",
            "data_cutoff": concept_cutoff,
            "lookback_days": len(concept_dates),
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "phase": None,
            "phase_desc": (
                f"概念榜只覆盖请求窗口的{date_coverage_pct:.1f}%，"
                f"低于{MIN_THEME_DATE_COVERAGE_PCT:.0f}%门槛，暂不判断轮动。"
            ),
            "rotation_score": None,
            "coverage": concept_coverage,
            "main_themes": main_themes,
            "concept_top_changes": concept_stats.get("rank_changes", []),
            "industry_top_changes": industry_stats.get("rank_changes", []),
        }
    if not available_daily_top10_complete:
        return {
            "status": "partial",
            "reason": "HOT_THEME_DAILY_TOP10_INCOMPLETE",
            "data_cutoff": concept_cutoff,
            "lookback_days": len(concept_dates),
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "phase": None,
            "phase_desc": (
                f"至少有一个概念榜交易日的有效TOP10少于"
                f"{MIN_THEME_DAILY_TOP10_COUNT}个，暂不判断轮动。"
            ),
            "rotation_score": None,
            "coverage": concept_coverage,
            "main_themes": main_themes,
            "concept_top_changes": concept_stats.get("rank_changes", []),
            "industry_top_changes": industry_stats.get("rank_changes", []),
        }

    # 轮动分数: 只比较请求窗口内相邻且样本完整的交易日。
    rotation_score = _calc_rotation_score(concept_df, trade_dates)
    if rotation_score is None:
        return {
            "status": "insufficient_history",
            "reason": "HOT_THEME_CONSECUTIVE_HISTORY_MISSING",
            "data_cutoff": concept_cutoff,
            "lookback_days": len(concept_dates),
            "requested_lookback_days": n_days,
            "date_range": [d1, d2],
            "phase": None,
            "phase_desc": "概念榜缺少可比较的相邻交易日，暂不判断轮动。",
            "rotation_score": None,
            "coverage": concept_coverage,
            "main_themes": main_themes,
            "concept_top_changes": concept_stats.get("rank_changes", []),
            "industry_top_changes": industry_stats.get("rank_changes", []),
        }
    if rotation_score < 30 and len(main_themes) >= 2:
        phase = "主线行情"
        phase_desc = f"市场存在较明确的主线，近{len(concept_dates)}个实际概念榜交易日'{main_themes[0]['name']}'等板块持续强势"
    elif rotation_score < 50:
        phase = "弱主线轮动"
        phase_desc = f"部分板块略占优势，但轮动较快，缺乏持续主线"
    elif rotation_score < 70:
        phase = "快速轮动"
        phase_desc = "板块切换频繁，近期热点持续性偏弱"
    else:
        phase = "极端轮动/混沌"
        phase_desc = "板块持续性很弱，当前市场方向缺少连续证据"

    return {
        "status": "ok",
        "data_cutoff": concept_cutoff,
        "lookback_days": len(concept_dates),
        "requested_lookback_days": n_days,
        "date_range": [d1, d2],
        "phase": phase,
        "phase_desc": phase_desc,
        "rotation_score": round(rotation_score, 1),
        "coverage": concept_coverage,
        "main_themes": main_themes,
        "concept_top_changes": concept_stats.get("rank_changes", []),
        "industry_top_changes": industry_stats.get("rank_changes", []),
    }


def _calc_theme_stats(df: pd.DataFrame, trade_dates: list[str], tag: str) -> dict:
    if df.empty:
        return {"theme_scores": [], "rank_changes": []}

    df = _normalize_date_col(df, "snapshot_date")
    df = df[df["concept_name"].notna() & (df["concept_name"].str.strip() != "")]
    if df.empty:
        return {"theme_scores": [], "rank_changes": []}

    n_days = len(trade_dates)
    dates_list = sorted(df["_datestr"].unique().tolist())

    # 统计每个概念在TOP N中的出现天数
    top20 = df[df["rank"] <= 20]
    top10 = df[df["rank"] <= 10]
    top5 = df[df["rank"] <= 5]

    agg_cols = ["concept_name", "concept_code"]

    def _agg(sub_df, col_prefix):
        g = sub_df.groupby(agg_cols, as_index=False).agg(
            appear_days=("snapshot_date", "nunique"),
            avg_rank=("rank", "mean"),
            avg_change_pct=("change_pct", "mean"),
            hot_value_total=("hot_value", "sum"),
        )
        g.columns = agg_cols + [f"{col_prefix}_{c}" for c in g.columns[2:]]
        return g

    t20 = _agg(top20, "t20")
    t10 = _agg(top10, "t10")
    t5 = _agg(top5, "t5")

    merged = t20.merge(t10, on=agg_cols, how="left").merge(t5, on=agg_cols, how="left")
    for c in merged.columns:
        if c not in agg_cols:
            merged[c] = merged[c].fillna(0)

    # 综合评分: 出现天数越多、排名越靠前、涨幅越好, 分数越高
    merged["score"] = (
        merged["t5_appear_days"] * 3.0
        + merged["t10_appear_days"] * 1.5
        + (merged["t20_appear_days"] - merged["t10_appear_days"]) * 0.5
    )
    merged["score"] += np.maximum(0, 100 - merged["t5_avg_rank"].clip(lower=1)) * 0.5
    merged["score"] += merged["t5_avg_change_pct"].clip(lower=0) * 0.3

    # 最近排名趋势
    latest_ranks = {}
    for _, r in df.iterrows():
        d = r["_datestr"]
        name = r["concept_name"]
        if name not in latest_ranks:
            latest_ranks[name] = {}
        latest_ranks[name][d] = int(r["rank"])

    # 排名变化: 最近2日的排名变动
    sorted_dates = sorted(df["_datestr"].unique().tolist())
    rank_changes = []
    if len(sorted_dates) >= 2:
        d1, d2 = sorted_dates[-2], sorted_dates[-1]
        d1_ranks = {r["concept_name"]: r["rank"] for _, r in df[df["_datestr"] == d1].iterrows()}
        d2_ranks = {r["concept_name"]: r["rank"] for _, r in df[df["_datestr"] == d2].iterrows()}
        changes = []
        for name in set(list(d1_ranks.keys()) + list(d2_ranks.keys())):
            r1 = d1_ranks.get(name, 999)
            r2 = d2_ranks.get(name, 999)
            diff = r1 - r2
            if abs(diff) >= 3:
                changes.append({
                    "name": name,
                    "prev_rank": r1,
                    "curr_rank": r2,
                    "change": diff,
                    "direction": "上升" if diff > 0 else "下降",
                })
        changes.sort(key=lambda x: -abs(x["change"]))
        rank_changes = changes[:10]

    theme_scores = merged[merged["score"] > 0].sort_values("score", ascending=False).head(15)

    result_scores = []
    for _, r in theme_scores.iterrows():
        name = r["concept_name"]
        recent = latest_ranks.get(name, {})
        recent_sorted = [recent[d] for d in sorted(recent.keys())]
        result_scores.append({
            "name": name,
            "code": r["concept_code"],
            "type": tag,
            "appear_days": int(r["t20_appear_days"]),
            "top5_days": int(r["t5_appear_days"]),
            "top10_days": int(r["t10_appear_days"]),
            "avg_rank": float(r["t5_avg_rank"]) if r["t5_appear_days"] > 0 else float(r["t10_avg_rank"]),
            "avg_change_pct": float(r["t5_avg_change_pct"]),
            "hot_value_total": float(r.get("t5_hot_value_total", r.get("t10_hot_value_total", 0))),
            "score": float(r["score"]),
            "recent_ranks": recent_sorted[-5:] if recent_sorted else [],
        })

    return {"theme_scores": result_scores, "rank_changes": rank_changes}


def _calc_rotation_score(df: pd.DataFrame, trade_dates: list[str]) -> float | None:
    """
    轮动强度 0-100:
      0 = 完全不轮动(同一板块始终霸榜)
      100 = 极致轮动(每天TOP10完全换血)
    """
    if df.empty or len(trade_dates) < 2:
        return None

    df = _normalize_date_col(df, "snapshot_date")
    available_dates = set(df["_datestr"].unique().tolist())
    requested_dates = sorted({_to_date_str(value) for value in trade_dates})
    if len(available_dates) < 2:
        return None

    # 计算相邻日TOP10的Jaccard差异
    similarities = []
    new_entry_rates = []
    stable_ratios = []
    rank_volatilities = []

    for i in range(1, len(requested_dates)):
        d_prev, d_curr = requested_dates[i - 1], requested_dates[i]
        if d_prev not in available_dates or d_curr not in available_dates:
            continue
        prev_set = set(df[df["_datestr"] == d_prev]["concept_name"].unique())
        curr_set = set(df[df["_datestr"] == d_curr]["concept_name"].unique())

        # 必须按 rank 排序后再取 TOP N，.unique() 不保序会导致 [:5] 取到错误的板块
        prev_top10_df = df[(df["_datestr"] == d_prev) & (df["rank"] <= 10)].sort_values("rank")
        curr_top10_df = df[(df["_datestr"] == d_curr) & (df["rank"] <= 10)].sort_values("rank")
        prev_top10 = prev_top10_df["concept_name"].tolist()
        curr_top10 = curr_top10_df["concept_name"].tolist()

        if len(prev_top10) == 0:
            continue

        intersection = len(set(prev_top10) & set(curr_top10))
        union = len(set(prev_top10) | set(curr_top10))
        if union > 0:
            similarities.append(intersection / union)

        new_entries = len(set(curr_top10) - set(prev_top10))
        new_entry_rates.append(new_entries / len(curr_top10) if len(curr_top10) > 0 else 1)

        stable = len(set(curr_top10[:5]) & set(prev_top10[:5])) / 5
        stable_ratios.append(stable)

        # 排名波动
        prev_rank_map = {}
        for _, r in df[(df["_datestr"] == d_prev) & (df["rank"] <= 20)].iterrows():
            prev_rank_map[r["concept_name"]] = r["rank"]
        curr_rank_map = {}
        for _, r in df[(df["_datestr"] == d_curr) & (df["rank"] <= 20)].iterrows():
            curr_rank_map[r["concept_name"]] = r["rank"]
        common = set(prev_rank_map.keys()) & set(curr_rank_map.keys())
        if common:
            rank_diffs = [abs(prev_rank_map[n] - curr_rank_map[n]) for n in common]
            rank_volatilities.append(np.mean(rank_diffs))

    if not similarities:
        return None

    avg_similarity = np.mean(similarities) if similarities else 0.5
    avg_new_rate = np.mean(new_entry_rates) if new_entry_rates else 0.5
    avg_stable = np.mean(stable_ratios) if stable_ratios else 0.5
    avg_rank_vol = np.mean(rank_volatilities) if rank_volatilities else 5

    norm_rank_vol = min(avg_rank_vol / 10, 1.0)

    rotation = (1 - avg_similarity) * 40 + avg_new_rate * 30 + (1 - avg_stable) * 20 + norm_rank_vol * 10
    return min(max(rotation, 0), 100)


# ═══════════════════════════════════════════
# 2. 大小盘风格分析
# ═══════════════════════════════════════════

def analyze_style(engine, trade_dates: list[str]) -> dict:
    """
    大小盘风格:
      - 大小盘结论只使用现有宽基指数的区间表现
      - 个股成交额分组只作为活跃度旁证，不冒充市值大小盘
    """
    if not trade_dates:
        return {"status": "no_data", "message": "无交易日数据"}

    d1 = trade_dates[0]
    d2 = trade_dates[-1]

    # 2.1 加载涨跌+成交额数据，在Python端按成交额分大盘/小盘（兼容MySQL 5.7）
    raw_q = text("""
        SELECT stock_code, trade_date, change_pct, amount
        FROM sm_stock_kline
        WHERE trade_date >= :d1 AND trade_date <= :d2 AND k_type = 1
          AND adjust_type = 0 AND amount > 0
          AND stock_code REGEXP '^(0|3|6)[0-9]{5}$'
        ORDER BY trade_date, amount
    """)
    raw_df = pd.read_sql(raw_q, engine, params={"d1": d1, "d2": d2})
    if not raw_df.empty:
        raw_df = raw_df.copy()
        raw_df["change_pct"] = pd.to_numeric(raw_df["change_pct"], errors="coerce")
        raw_df["amount"] = pd.to_numeric(raw_df["amount"], errors="coerce")
        raw_df = raw_df[
            np.isfinite(raw_df["change_pct"])
            & np.isfinite(raw_df["amount"])
            & (raw_df["amount"] > 0)
        ]

    expected_breadth_stock_count = None
    try:
        breadth_universe_q = text("""
            SELECT COUNT(DISTINCT stock_code) AS expected_stock_cnt
            FROM si_all_code
            WHERE stock_code REGEXP '^(0|3|6)[0-9]{5}$'
        """)
        breadth_universe_df = pd.read_sql(breadth_universe_q, engine)
        if not breadth_universe_df.empty:
            expected_value = pd.to_numeric(
                breadth_universe_df.iloc[0].get("expected_stock_cnt"),
                errors="coerce",
            )
            if pd.notna(expected_value) and float(expected_value) > 0:
                expected_breadth_stock_count = int(expected_value)
    except Exception:
        expected_breadth_stock_count = None

    daily_style = {}
    market_df_rows = []
    if not raw_df.empty:
        for td, grp in raw_df.groupby('trade_date'):
            td_str = _to_date_str(td)
            n = len(grp)
            large_cut = int(n * 0.33)
            small_cut = int(n * 0.67) + 1
            avg_chg = grp['change_pct'].mean()
            up_ratio = (grp['change_pct'] > 0).mean()
            large = grp.iloc[:large_cut]  # 最小的33%成交额 = 小盘... 不对，ORDER BY amount升序，前33%是小盘
            # 修正：按成交额降序排，前33%是大盘
            grp_sorted = grp.sort_values('amount', ascending=False)
            large = grp_sorted.iloc[:large_cut]
            small = grp_sorted.iloc[small_cut:]
            market_df_rows.append({"trade_date": td_str, "avg_chg": avg_chg, "up_ratio": up_ratio})
            if large.empty or small.empty:
                continue
            daily_style[td_str] = {
                "avg_chg": round(float(avg_chg or 0), 4),
                "up_ratio": round(float(up_ratio or 0), 4),
                "large_chg": round(float(large['change_pct'].mean() or 0), 4),
                "large_cnt": int(len(large)),
                "small_chg": round(float(small['change_pct'].mean() or 0), 4),
                "small_cnt": int(len(small)),
                "mid_chg": 0,
                "mid_cnt": 0,
            }
    style_df = pd.DataFrame(market_df_rows) if market_df_rows else pd.DataFrame()

    market_df = style_df

    # 2.3 大小盘合计
    large_total_chg = 0
    small_total_chg = 0
    large_days = 0
    small_days = 0
    recent_large = 0
    recent_small = 0
    available_style_dates = [td for td in trade_dates if td in daily_style]
    half_point = len(available_style_dates) // 2

    for i, td in enumerate(available_style_dates):
        v = daily_style[td]
        lc = float(v["large_chg"])
        sc = float(v["small_chg"])
        large_total_chg += lc
        large_days += 1
        small_total_chg += sc
        small_days += 1
        if i >= half_point:
            recent_large += lc
            recent_small += sc

    large_avg_daily = round(large_total_chg / large_days, 2) if large_days > 0 else 0
    small_avg_daily = round(small_total_chg / small_days, 2) if small_days > 0 else 0
    diff = round(small_avg_daily - large_avg_daily, 2)

    # 近期趋势
    recent_count = len(available_style_dates) - half_point
    recent_large_avg = round(recent_large / max(recent_count, 1), 2)
    recent_small_avg = round(recent_small / max(recent_count, 1), 2)

    large_momentum = "走平"
    if recent_large_avg > large_avg_daily + 0.1:
        large_momentum = "大面积走强"
    elif recent_large_avg < large_avg_daily - 0.1:
        large_momentum = "大面积走弱"

    small_momentum = "走平"
    if recent_small_avg > small_avg_daily + 0.1:
        small_momentum = "小幅走强"
    elif recent_small_avg < small_avg_daily - 0.1:
        small_momentum = "小幅走弱"

    # 2.4 大小盘相对强弱判定
    if diff > 0.3:
        bias = "小盘强势"
        bias_desc = f"小盘日均{small_avg_daily:+.2f}%, 显著跑赢大盘{large_avg_daily:+.2f}%, 市场偏好题材炒作"
    elif diff > 0.05:
        bias = "小盘偏强"
        bias_desc = f"小盘日均{small_avg_daily:+.2f}%, 略强于大盘{large_avg_daily:+.2f}%"
    elif diff > -0.05:
        bias = "大小均衡"
        bias_desc = f"大小盘日均涨跌接近(大盘{large_avg_daily:+.2f}% / 小盘{small_avg_daily:+.2f}%), 风格不明确"
    elif diff > -0.3:
        bias = "大盘偏强"
        bias_desc = f"大盘日均{large_avg_daily:+.2f}%, 略强于小盘{small_avg_daily:+.2f}%, 资金偏向蓝筹"
    else:
        bias = "大盘强势"
        bias_desc = f"大盘日均{large_avg_daily:+.2f}%, 显著跑赢小盘{small_avg_daily:+.2f}%, 资金避险偏好核心资产"

    # 2.5 指数明细(尝试 sm_index_kline, 失败则用大小盘统计数据)
    indices = []
    large_idx = ["000016", "000300"]
    small_idx = ["000905", "000852", "399303"]
    all_idx = large_idx + small_idx + ["399006", "000688"]
    index_placeholders = ",".join(f":style_index_{index}" for index, _code in enumerate(all_idx))
    index_params = {
        "d1": d1,
        "d2": d2,
        **{f"style_index_{index}": code for index, code in enumerate(all_idx)},
    }

    try:
        idx_q = text(f"""
            SELECT index_code, trade_date, close, change_pct
            FROM sm_index_kline
            WHERE index_code IN ({index_placeholders})
              AND trade_date >= :d1 AND trade_date <= :d2 AND k_type = 1
            ORDER BY index_code, trade_date
        """)
        idx_df = pd.read_sql(idx_q, engine, params=index_params)
    except Exception:
        idx_df = pd.DataFrame()

    if idx_df.empty:
        try:
            current_q = text(f"""
                SELECT index_code, trade_date, price AS close, change_pct
                FROM sm_index_current
                WHERE index_code IN ({index_placeholders})
            """)
            idx_df = pd.read_sql(current_q, engine, params=index_params)
        except Exception:
            idx_df = pd.DataFrame()

    if idx_df.empty:
        # 成交额不是市值；缺少宽基指数时明确不可用，不构造虚拟指数。
        indices = []
    else:
        idx_df = idx_df.copy()
        idx_df["index_code"] = idx_df["index_code"].map(
            lambda value: str(value or "").split(".")[0].zfill(6)
        )
        idx_df["close"] = pd.to_numeric(idx_df["close"], errors="coerce")
        idx_df["change_pct"] = pd.to_numeric(idx_df["change_pct"], errors="coerce")
        idx_df["_datestr"] = idx_df["trade_date"].map(_to_date_str)
        for code in all_idx:
            sub = idx_df[idx_df["index_code"] == code].sort_values("_datestr")
            # Only compare indices that cover the same requested endpoints.
            # Averaging returns over different start/end dates creates a false
            # size-style spread when an index has a data gap.
            endpoints = sub[sub["_datestr"].isin({d1, d2})].sort_values("_datestr")
            single_session = d1 == d2
            if single_session:
                if len(endpoints) != 1 or endpoints["_datestr"].iloc[0] != d1:
                    continue
                if not (
                    np.isfinite(endpoints["close"].iloc[0])
                    and endpoints["close"].iloc[0] > 0
                    and np.isfinite(endpoints["change_pct"].iloc[0])
                ):
                    continue
            elif len(endpoints) < 2 or set(endpoints["_datestr"].tolist()) != {d1, d2}:
                continue
            elif not (
                np.isfinite(endpoints["close"].iloc[0])
                and endpoints["close"].iloc[0] > 0
                and np.isfinite(endpoints["close"].iloc[-1])
                and endpoints["close"].iloc[-1] > 0
            ):
                continue
            name = INDEX_MAP.get(code, code)
            first_close = float(endpoints["close"].iloc[0])
            last_close = float(endpoints["close"].iloc[-1])
            total_chg = (
                float(endpoints["change_pct"].iloc[-1])
                if single_session
                else ((last_close - first_close) / first_close * 100 if first_close else 0)
            )
            valid_changes = sub.loc[np.isfinite(sub["change_pct"]), "change_pct"]
            avg_daily_chg = float(valid_changes.mean()) if not valid_changes.empty else None
            win_rate = float((valid_changes > 0).mean() * 100) if not valid_changes.empty else None

            price_sub = sub[np.isfinite(sub["close"]) & (sub["close"] > 0)]
            half = len(price_sub) // 2
            first_half = price_sub.iloc[:half] if half > 0 else price_sub.iloc[:1]
            second_half = price_sub.iloc[half:]
            h1_chg = 0
            h2_chg = 0
            if len(first_half) > 1:
                h1_close_f = float(first_half["close"].iloc[0])
                h1_close_l = float(first_half["close"].iloc[-1])
                h1_chg = (h1_close_l - h1_close_f) / h1_close_f * 100 if h1_close_f else 0
            if len(second_half) > 1:
                h2_close_f = float(second_half["close"].iloc[0])
                h2_close_l = float(second_half["close"].iloc[-1])
                h2_chg = (h2_close_l - h2_close_f) / h2_close_f * 100 if h2_close_f else 0

            momentum = "走平"
            if h2_chg > h1_chg + 1:
                momentum = "加速上涨" if h2_chg > 0 else "跌幅收窄"
            elif h2_chg < h1_chg - 1:
                momentum = "涨幅放缓" if h2_chg > 0 else "加速下跌"

            indices.append({
                "code": code,
                "name": name,
                "category": "大" if code in large_idx else "小" if code in small_idx else "其他",
                "total_change_pct": round(total_chg, 2),
                "avg_daily_chg": round(avg_daily_chg, 2) if avg_daily_chg is not None else None,
                "win_rate": round(win_rate, 1) if win_rate is not None else None,
                "half1_change_pct": round(h1_chg, 2),
                "half2_change_pct": round(h2_chg, 2),
                "momentum": momentum,
                "last_price": round(last_close, 2),
                "last_change_pct": (
                    round(float(endpoints["change_pct"].iloc[-1]), 2)
                    if np.isfinite(endpoints["change_pct"].iloc[-1])
                    else None
                ),
                "date_range": [d1, d2],
            })

    large_index_returns = [float(item["total_change_pct"]) for item in indices if item["category"] == "大"]
    small_index_returns = [float(item["total_change_pct"]) for item in indices if item["category"] == "小"]
    if large_index_returns and small_index_returns:
        used_large = [item["code"] for item in indices if item["category"] == "大"]
        used_small = [item["code"] for item in indices if item["category"] == "小"]
        complete_groups = set(used_large) == set(large_idx) and set(used_small) == set(small_idx)
        large_index_return = round(float(np.mean(large_index_returns)), 2)
        small_index_return = round(float(np.mean(small_index_returns)), 2)
        index_diff = round(small_index_return - large_index_return, 2)
        if index_diff > 1.0:
            bias = "小盘占优"
        elif index_diff < -1.0:
            bias = "大盘占优"
        else:
            bias = "大小盘均衡"
        bias_desc = (
            f"现有小盘宽基区间平均{small_index_return:+.2f}%，"
            f"大盘宽基区间平均{large_index_return:+.2f}%，差值{index_diff:+.2f}%。"
        )
        size_style = {
            "status": "available" if complete_groups else "partial",
            "bias": bias,
            "small_minus_large_pct": index_diff,
            "large_index_return_pct": large_index_return,
            "small_index_return_pct": small_index_return,
            "method": (
                "同一交易日可用大盘宽基与小盘宽基涨跌幅均值之差"
                if d1 == d2
                else "可用大盘宽基与可用小盘宽基在同一首末交易日的区间涨跌均值之差"
            ),
            "data_cutoff": d2,
            "date_range": [d1, d2],
            "lookback_days": len(trade_dates),
            "coverage": {
                "large": {"used": used_large, "expected": large_idx},
                "small": {"used": used_small, "expected": small_idx},
            },
            "evidence": [
                f"大盘组区间均值{large_index_return:+.2f}%",
                f"小盘组区间均值{small_index_return:+.2f}%",
                f"共同区间{d1}至{d2}",
            ],
            "reason": None if complete_groups else "INCOMPLETE_BROAD_INDEX_GROUP",
        }
        diff = index_diff
    else:
        bias = "大小盘数据不可用"
        bias_desc = "缺少可比的大盘与小盘宽基指数K线；成交额分组不替代市值风格。"
        size_style = {
            "status": "unavailable",
            "reason": "RELIABLE_BROAD_INDEX_PAIR_MISSING",
            "method": "需要大盘组与小盘组至少各一个覆盖相同首末交易日的宽基指数",
            "data_cutoff": d2,
            "lookback_days": len(trade_dates),
        }
        diff = None

    # 2.6 市场整体活跃度
    requested_dates = sorted({_to_date_str(value) for value in trade_dates})
    valid_breadth_counts: dict[str, int] = {}
    if not raw_df.empty:
        for td, grp in raw_df.groupby("trade_date"):
            td_str = _to_date_str(td)
            if td_str not in requested_dates:
                continue
            valid_breadth_counts[td_str] = int(
                grp["stock_code"].nunique()
                if "stock_code" in grp.columns
                else len(grp)
            )
    breadth_coverage_rows = []
    for trade_date_value in requested_dates:
        valid_count = valid_breadth_counts.get(trade_date_value, 0)
        coverage_pct = (
            valid_count / expected_breadth_stock_count * 100
            if expected_breadth_stock_count
            else None
        )
        breadth_coverage_rows.append(
            {
                "trade_date": trade_date_value,
                "valid_stock_count": valid_count,
                "expected_stock_count": expected_breadth_stock_count,
                "coverage_pct": round(coverage_pct, 1) if coverage_pct is not None else None,
            }
        )
    breadth_available_dates = sorted(valid_breadth_counts)
    breadth_actual_cutoff = breadth_available_dates[-1] if breadth_available_dates else None
    breadth_coverage_verifiable = expected_breadth_stock_count is not None
    breadth_window_complete = (
        breadth_coverage_verifiable
        and breadth_actual_cutoff == d2
        and all(
            item["coverage_pct"] is not None
            and item["coverage_pct"] >= MIN_MARKET_BREADTH_STOCK_COVERAGE_PCT
            for item in breadth_coverage_rows
        )
    )
    breadth_coverage = {
        "requested_trade_days": len(requested_dates),
        "available_trade_days": len(breadth_available_dates),
        "missing_trade_dates": sorted(set(requested_dates) - set(breadth_available_dates)),
        "minimum_stock_coverage_pct": MIN_MARKET_BREADTH_STOCK_COVERAGE_PCT,
        "stock_coverage_status": (
            "complete"
            if breadth_window_complete
            else "incomplete" if breadth_coverage_verifiable else "unavailable"
        ),
        "stock_coverage_by_date": breadth_coverage_rows,
        "requested_cutoff_covered": breadth_actual_cutoff == d2,
    }

    market_activity = {
        "status": "unavailable",
        "reason": "MARKET_BREADTH_DATA_MISSING",
        "data_cutoff": breadth_actual_cutoff,
        "coverage": breadth_coverage,
        "window_days": 0,
    }
    if not market_df.empty:
        market_df_sorted = market_df.sort_values("trade_date")
        recent = market_df_sorted.tail(3)
        market_activity = {
            "status": "available" if breadth_window_complete else "partial",
            "reason": (
                None
                if breadth_window_complete
                else "MARKET_BREADTH_REQUESTED_CUTOFF_MISSING"
                if breadth_actual_cutoff != d2
                else "MARKET_BREADTH_STOCK_COVERAGE_INCOMPLETE"
                if breadth_coverage_verifiable
                else "MARKET_BREADTH_STOCK_COVERAGE_UNAVAILABLE"
            ),
            "data_cutoff": breadth_actual_cutoff,
            "coverage": breadth_coverage,
            "recent_avg_chg": round(float(recent["avg_chg"].dropna().mean()) if not recent["avg_chg"].dropna().empty else 0, 2),
            "recent_up_ratio": round(float(recent["up_ratio"].dropna().mean()) * 100 if not recent["up_ratio"].dropna().empty else 50, 1),
            "window_avg_chg": round(float(market_df_sorted["avg_chg"].dropna().mean()) if not market_df_sorted["avg_chg"].dropna().empty else 0, 2),
            "window_up_ratio": round(float(market_df_sorted["up_ratio"].dropna().mean()) * 100 if not market_df_sorted["up_ratio"].dropna().empty else 50, 1),
            "window_days": int(len(market_df_sorted)),
        }

    return {
        "status": "ok",
        "lookback_days": len(trade_dates),
        "date_range": [d1, d2],
        "bias": bias,
        "bias_desc": bias_desc,
        "large_small_diff": diff,
        "large_avg_daily": large_avg_daily,
        "small_avg_daily": small_avg_daily,
        "indices": indices,
        "market_activity": market_activity,
        "size_style": size_style,
        "liquidity_activity_proxy": {
            "status": "available" if daily_style else "unavailable",
            "high_turnover_avg_daily_pct": large_avg_daily,
            "low_turnover_avg_daily_pct": small_avg_daily,
            "note": "按个股成交额分组，仅用于活跃度旁证，不代表大盘/小盘市值风格。",
        },
        "growth_value_style": {
            "status": "unavailable",
            "reason": "RELIABLE_GROWTH_VALUE_CLASSIFICATION_MISSING",
            "note": "现有数据没有可复算的成长/价值分类或成对指数，不输出猜测结论。",
        },
    }


# ═══════════════════════════════════════════
# 3. 资金风格分析
# ═══════════════════════════════════════════

def analyze_capital_style(engine, trade_dates: list[str]) -> dict:
    """
    资金流向风格:
      - 统计主力资金总体净流入情况
      - 从北向资金判断外资态度
    """
    if not trade_dates:
        return {"status": "no_data"}

    d1 = trade_dates[0]
    d2 = trade_dates[-1]

    # 主力资金日度汇总
    flow_q = text("""
        SELECT trade_date,
               SUM(main_net_inflow) AS total_main_flow,
               COUNT(main_net_inflow) AS stock_cnt,
               SUM(CASE WHEN main_net_inflow > 0 THEN 1 ELSE 0 END) AS inflow_cnt
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= :d1 AND trade_date <= :d2
          AND stock_code REGEXP '^(0|3|6)[0-9]{5}$'
        GROUP BY trade_date
        ORDER BY trade_date
    """)
    flow_df = pd.read_sql(flow_q, engine, params={"d1": d1, "d2": d2})

    if flow_df.empty:
        return {"status": "no_data", "message": "资金流向数据为空"}

    expected_stock_count = None
    try:
        expected_q = text("""
            SELECT COUNT(DISTINCT stock_code) AS expected_stock_cnt
            FROM si_all_code
            WHERE stock_code REGEXP '^(0|3|6)[0-9]{5}$'
        """)
        expected_df = pd.read_sql(expected_q, engine)
        if not expected_df.empty:
            expected_value = pd.to_numeric(
                expected_df.iloc[0].get("expected_stock_cnt"), errors="coerce"
            )
            if pd.notna(expected_value) and float(expected_value) > 0:
                expected_stock_count = int(expected_value)
    except Exception:
        expected_stock_count = None

    flow_df = flow_df.copy()
    flow_df["_datestr"] = flow_df["trade_date"].map(_to_date_str)
    for column in ("total_main_flow", "stock_cnt", "inflow_cnt"):
        flow_df[column] = pd.to_numeric(flow_df[column], errors="coerce")
    flow_df = flow_df[
        np.isfinite(flow_df["total_main_flow"])
        & np.isfinite(flow_df["stock_cnt"])
        & np.isfinite(flow_df["inflow_cnt"])
        & (flow_df["stock_cnt"] > 0)
    ].sort_values("_datestr")
    if flow_df.empty:
        return {"status": "no_data", "message": "资金流向数据没有可用数值"}

    requested_dates = sorted({_to_date_str(value) for value in trade_dates})
    available_dates = sorted(set(flow_df["_datestr"].tolist()) & set(requested_dates))
    flow_df = flow_df[flow_df["_datestr"].isin(available_dates)]
    if flow_df.empty:
        return {"status": "no_data", "message": "资金流向数据未覆盖请求窗口"}
    missing_dates = sorted(set(requested_dates) - set(available_dates))
    actual_cutoff = available_dates[-1]
    observed_stock_counts = {
        str(row["_datestr"]): int(row["stock_cnt"])
        for _, row in flow_df.iterrows()
    }
    stock_coverage_rows = []
    for trade_date_value in requested_dates:
        observed_count = observed_stock_counts.get(trade_date_value)
        coverage_pct = (
            observed_count / expected_stock_count * 100
            if observed_count is not None and expected_stock_count
            else None
        )
        stock_coverage_rows.append(
            {
                "trade_date": trade_date_value,
                "flow_stock_count": observed_count,
                "expected_stock_count": expected_stock_count,
                "coverage_pct": round(coverage_pct, 1) if coverage_pct is not None else None,
            }
        )
    stock_coverage_verifiable = expected_stock_count is not None
    stock_coverage_complete = stock_coverage_verifiable and all(
        item["coverage_pct"] is not None
        and item["coverage_pct"] >= MIN_CAPITAL_FLOW_STOCK_COVERAGE_PCT
        for item in stock_coverage_rows
    )
    coverage = {
        "requested_trade_days": len(requested_dates),
        "available_trade_days": len(available_dates),
        "missing_trade_dates": missing_dates,
        "date_coverage_pct": round(
            len(available_dates) / max(len(requested_dates), 1) * 100,
            1,
        ),
        "minimum_stock_coverage_pct": MIN_CAPITAL_FLOW_STOCK_COVERAGE_PCT,
        "stock_coverage_status": (
            "complete"
            if stock_coverage_complete
            else "incomplete" if stock_coverage_verifiable else "unavailable"
        ),
        "stock_coverage_by_date": stock_coverage_rows,
    }

    total_flow = float(flow_df["total_main_flow"].sum())
    day_count = len(flow_df)
    avg_daily_flow = total_flow / day_count if day_count > 0 else 0

    inflow_ratio = float(flow_df["inflow_cnt"].sum() / max(flow_df["stock_cnt"].sum(), 1))

    half_idx = len(flow_df) // 2
    recent_half = flow_df.iloc[half_idx:]
    recent_flow = float(recent_half["total_main_flow"].sum()) if len(recent_half) > 0 else 0

    if total_flow > 1e9:
        flow_style = "主力资金净流入"
    elif total_flow < -1e9:
        flow_style = "主力资金净流出"
    elif total_flow > 0:
        flow_style = "资金小幅净流入"
    elif total_flow < 0:
        flow_style = "资金小幅净流出"
    else:
        flow_style = "资金大致持平"

    recent_daily_flows = [float(value) for value in recent_half["total_main_flow"].tolist()]
    if len(recent_daily_flows) >= 2 and all(value > 0 for value in recent_daily_flows):
        recent_trend = "近期资金持续流入"
    elif len(recent_daily_flows) >= 2 and all(value < 0 for value in recent_daily_flows):
        recent_trend = "近期资金持续流出"
    elif recent_flow > 0:
        recent_trend = "近期资金偏流入"
    elif recent_flow < 0:
        recent_trend = "近期资金偏流出"
    else:
        recent_trend = "近期资金大致持平"

    complete_window = (
        actual_cutoff == d2
        and not missing_dates
        and stock_coverage_complete
    )
    status = "ok" if complete_window else "partial"
    if not complete_window:
        flow_style = None
        recent_trend = None

    # 北向资金
    north_total = None
    north_text = "北向资金数据不可用"
    north_status = "unavailable"
    north_cutoff = None
    north_coverage = {
        "requested_trade_days": len(requested_dates),
        "available_trade_days": 0,
        "missing_trade_dates": requested_dates,
    }
    try:
        north_q = text("""
            SELECT trade_date, net_flow
            FROM st_north_flow_daily
            WHERE trade_date >= :d1 AND trade_date <= :d2
            ORDER BY trade_date
        """)
        north_df = pd.read_sql(north_q, engine, params={"d1": d1, "d2": d2})
        if not north_df.empty and "net_flow" in north_df.columns:
            north_df = north_df.copy()
            north_df["_datestr"] = north_df["trade_date"].map(_to_date_str)
            north_df["net_flow"] = pd.to_numeric(north_df["net_flow"], errors="coerce")
            north_df = north_df[
                np.isfinite(north_df["net_flow"])
                & north_df["_datestr"].isin(requested_dates)
            ].sort_values("_datestr")
            north_dates = sorted(set(north_df["_datestr"].tolist()))
            north_missing_dates = sorted(set(requested_dates) - set(north_dates))
            north_cutoff = north_dates[-1] if north_dates else None
            north_coverage = {
                "requested_trade_days": len(requested_dates),
                "available_trade_days": len(north_dates),
                "missing_trade_dates": north_missing_dates,
            }
            if north_dates and north_cutoff == d2 and not north_missing_dates:
                north_status = "available"
                north_total = float(north_df["net_flow"].sum())
                if north_total > 1e9:
                    north_text = "北向资金大幅净流入"
                elif north_total > 0:
                    north_text = "北向资金小幅净流入"
                elif north_total < -1e9:
                    north_text = "北向资金大幅净流出"
                elif north_total < 0:
                    north_text = "北向资金小幅净流出"
                else:
                    north_text = "北向资金持平"
            elif north_dates:
                north_status = "partial"
                north_text = "北向资金窗口覆盖不完整，暂不判断方向"
    except Exception:
        pass

    return {
        "status": status,
        "reason": (
            None
            if complete_window
            else "CAPITAL_FLOW_WINDOW_INCOMPLETE"
            if actual_cutoff != d2 or missing_dates
            else "CAPITAL_FLOW_STOCK_COVERAGE_INCOMPLETE"
            if stock_coverage_verifiable
            else "CAPITAL_FLOW_STOCK_COVERAGE_UNAVAILABLE"
        ),
        "date_range": [d1, d2],
        "data_cutoff": actual_cutoff,
        "coverage": coverage,
        "flow_style": flow_style,
        "recent_trend": recent_trend,
        "north_flow_note": north_text,
        "total_main_flow": round(total_flow, 0),
        "avg_daily_flow": round(avg_daily_flow, 0),
        "inflow_ratio": round(inflow_ratio * 100, 1),
        "recent_daily_flows": [round(value, 0) for value in recent_daily_flows],
        "north_flow_status": north_status,
        "north_data_cutoff": north_cutoff,
        "north_coverage": north_coverage,
        "north_total_flow": round(north_total, 0) if north_total is not None else None,
    }


def build_style_dimensions(theme: dict, style: dict, capital: dict) -> dict:
    """Expose each style conclusion with its own evidence and availability."""
    activity = style.get("market_activity") or {}
    rotation_status = "available" if theme.get("status") == "ok" else "unavailable"
    capital_status = "available" if capital.get("status") == "ok" else "unavailable"
    breadth_ratio = activity.get("window_up_ratio", activity.get("recent_up_ratio"))
    breadth_change = activity.get("window_avg_chg", activity.get("recent_avg_chg"))
    try:
        breadth_ratio = float(breadth_ratio)
        if not np.isfinite(breadth_ratio):
            breadth_ratio = None
    except (TypeError, ValueError):
        breadth_ratio = None
    try:
        breadth_change = float(breadth_change)
        if not np.isfinite(breadth_change):
            breadth_change = None
    except (TypeError, ValueError):
        breadth_change = None
    breadth_days = int(
        activity.get("window_days")
        or min(3, int(style.get("lookback_days") or 0))
        or 0
    )
    activity_status = str(activity.get("status") or "").lower()
    breadth_status = (
        "available"
        if activity_status == "available" and breadth_ratio is not None
        else "partial"
        if activity_status == "partial" and breadth_ratio is not None
        else "unavailable"
    )
    return {
        "size": style.get("size_style") or {
            "status": "unavailable",
            "reason": "RELIABLE_BROAD_INDEX_PAIR_MISSING",
        },
        "capital": {
            "status": capital_status,
            "flow_style": capital.get("flow_style") if capital_status == "available" else None,
            "recent_trend": capital.get("recent_trend") if capital_status == "available" else None,
            "inflow_stock_ratio_pct": capital.get("inflow_ratio") if capital_status == "available" else None,
            "method": "现有个股主力资金日表按交易日汇总；北向资金仅作独立旁证",
            "data_cutoff": capital.get("data_cutoff") if capital_status == "available" else None,
            "evidence": (
                [
                    f"累计主力净流入{float(capital.get('total_main_flow') or 0):.0f}",
                    f"净流入个股占比{float(capital.get('inflow_ratio') or 0):.1f}%",
                ]
                if capital_status == "available"
                else []
            ),
            "reason": (
                None
                if capital_status == "available"
                else capital.get("reason") or "CAPITAL_FLOW_DATA_MISSING"
            ),
        },
        "rotation": {
            "status": rotation_status,
            "score": theme.get("rotation_score") if rotation_status == "available" else None,
            "phase": theme.get("phase") if rotation_status == "available" else None,
            "method": "相邻交易日热门概念TOP10重合、新进、前五稳定度与共同概念排名波动",
            "data_cutoff": (
                theme.get("data_cutoff")
                or (theme.get("date_range") or [None])[-1]
                if rotation_status == "available"
                else None
            ),
            "lookback_days": theme.get("lookback_days"),
            "evidence": ([theme.get("phase_desc")] if rotation_status == "available" and theme.get("phase_desc") else []),
            "reason": None if rotation_status == "available" else "HOT_THEME_HISTORY_MISSING",
        },
        "breadth": {
            "status": breadth_status,
            "recent_up_ratio_pct": breadth_ratio if breadth_status == "available" else None,
            "recent_avg_change_pct": breadth_change if breadth_status == "available" else None,
            "method": "现有全市场日K中成交额大于0的上涨股票数/有效股票数，在本次实际窗口内逐日平均",
            "data_cutoff": activity.get("data_cutoff"),
            "lookback_days": breadth_days or None,
            "coverage": activity.get("coverage") or {},
            "evidence": (
                [f"{breadth_days}日窗口上涨占比{float(breadth_ratio or 0):.1f}%"]
                if breadth_status == "available"
                else []
            ),
            "reason": None if breadth_status == "available" else activity.get("reason") or "MARKET_BREADTH_DATA_MISSING",
        },
        "growth_value": style.get("growth_value_style") or {
            "status": "unavailable",
            "reason": "RELIABLE_GROWTH_VALUE_CLASSIFICATION_MISSING",
        },
    }


# ═══════════════════════════════════════════
# 综合报告
# ═══════════════════════════════════════════

def run_full_analysis(lookback_days: int = 20, end_date: str = None, top_n: int = 5, engine=None) -> dict:
    if engine is None:
        engine = _engine()

    if end_date is None:
        end_date = date.today().isoformat()

    trade_dates = _get_trade_dates(engine, end_date, lookback_days)
    if not trade_dates:
        return {"error": f"未找到 {end_date} 之前的交易日数据"}

    theme_result = analyze_main_theme(engine, trade_dates, top_n)
    style_result = analyze_style(engine, trade_dates)
    capital_result = analyze_capital_style(engine, trade_dates)

    return {
        "analysis_date": end_date,
        "lookback_days": len(trade_dates),
        "trade_dates": trade_dates,
        "latest_date": trade_dates[-1] if trade_dates else None,
        "theme_analysis": theme_result,
        "style_analysis": style_result,
        "capital_analysis": capital_result,
        "style_dimensions": build_style_dimensions(theme_result, style_result, capital_result),
    }


def format_report(result: dict) -> str:
    lines = []
    sep = "=" * 60
    lines.append(sep)
    lines.append(f"  市场情绪与风格分析报告")
    lines.append(f"  分析日期: {result['analysis_date']}  |  回顾: {result['lookback_days']}个交易日")
    lines.append(f"  数据区间: {result.get('trade_dates', [])[0] if result.get('trade_dates') else 'N/A'} ~ "
                 f"{result.get('trade_dates', [])[-1] if result.get('trade_dates') else 'N/A'}")
    lines.append(sep)

    # 主线/轮动
    theme = result.get("theme_analysis", {})
    if theme.get("status") == "ok":
        lines.append("")
        lines.append("─── 一、主线与轮动分析 ───")
        lines.append(f"  市场阶段: {theme['phase']}")
        lines.append(f"  轮动强度: {theme['rotation_score']}/100 ({'低' if theme['rotation_score'] < 30 else '中等' if theme['rotation_score'] < 60 else '高'})")
        lines.append(f"  解读: {theme['phase_desc']}")
        lines.append("")

        main_themes = theme.get("main_themes", [])
        if main_themes:
            lines.append(f"  近期强势板块 TOP{len(main_themes)}:")
            lines.append(f"  {'排名':<6}{'板块':<12}{'类型':<6}{'出现天数':<10}{'均排名':<8}{'均涨幅':<10}{'得分'}")
            lines.append(f"  {'-' * 56}")
            for i, t in enumerate(main_themes, 1):
                lines.append(
                    f"  {i:<6}{t['name']:<12}{t['type']:<6}"
                    f"{t['appear_days']}/{theme.get('lookback_days', 0):<7}"
                    f"{t['avg_rank']:<8}{t['avg_change_pct']:+.2f}%{'':>4}"
                    f"{t['score']:.1f}"
                )

        # 排名异动
        rank_changes = theme.get("concept_top_changes", [])
        if rank_changes:
            lines.append("")
            lines.append("  概念排名异动(日间变化>=3):")
            for c in rank_changes[:5]:
                arrow = "↑" if c["change"] > 0 else "↓"
                lines.append(f"    {c['name']:<12} {c['prev_rank']}→{c['curr_rank']} {arrow}{abs(c['change'])}")

    # 风格
    style = result.get("style_analysis", {})
    if style.get("status") == "ok":
        lines.append("")
        lines.append("─── 二、大小盘风格分析 ───")
        lines.append(f"  风格判定: {style['bias']}")
        if style.get("large_small_diff") is not None:
            lines.append(f"  大小盘差值: {style['large_small_diff']:+.1f}% (正值=小盘强)")
        else:
            lines.append("  大小盘差值: 不可用")
        lines.append(f"  解读: {style['bias_desc']}")
        lines.append("")

        indices = style.get("indices", [])
        if indices:
            lines.append(f"  各指数近期表现:")
            lines.append(f"  {'指数':<12}{'类别':<6}{'区间涨跌':<12}{'胜率':<8}{'趋势':<10}{'最新价':<10}{'当日涨跌'}")
            lines.append(f"  {'-' * 68}")
            for idx in indices:
                win_rate_text = (
                    f"{float(idx['win_rate']):.1f}%"
                    if idx.get("win_rate") is not None
                    else "-"
                )
                last_change_text = (
                    f"{float(idx['last_change_pct']):+.2f}%"
                    if idx.get("last_change_pct") is not None
                    else "-"
                )
                lines.append(
                    f"  {idx['name']:<12}{idx['category']:<6}"
                    f"{idx['total_change_pct']:+.2f}%{'':>4}"
                    f"{win_rate_text:<12}"
                    f"{idx['momentum']:<10}"
                    f"{idx['last_price']:<10}"
                    f"{last_change_text}"
                )
        else:
            lines.append("  (指数K线数据暂不可用，不输出大小盘判断)")

        activity = style.get("market_activity", {})
        if activity.get("status") == "available":
            lines.append("")
            lines.append(f"  近3日市场活跃度: 均涨幅 {activity.get('recent_avg_chg', 0):+.2f}%  "
                         f"上涨比 {activity.get('recent_up_ratio', 0):.1f}%")
        elif activity:
            lines.append("")
            lines.append(f"  市场宽度不可用: {activity.get('reason') or '样本覆盖不足'}")

    # 资金
    capital = result.get("capital_analysis", {})
    if capital.get("status") == "ok":
        lines.append("")
        lines.append("─── 三、资金风格分析 ───")
        lines.append(f"  总体风格: {capital.get('flow_style', '')}")
        lines.append(f"  近期趋势: {capital.get('recent_trend', '')}")
        lines.append(f"  主力净流入合计: {capital.get('total_main_flow', 0):,.0f}")
        lines.append(f"  日均主力净流入: {capital.get('avg_daily_flow', 0):,.0f}")
        lines.append(f"  主力流入个股占比: {capital.get('inflow_ratio', 0):.1f}%")
        north = capital.get('north_flow_note', '')
        if north:
            north_total = capital.get("north_total_flow")
            lines.append(
                f"  {north}: {float(north_total):,.0f}"
                if north_total is not None
                else f"  {north}"
            )

    lines.append("")
    lines.append(sep)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="市场情绪与风格分析")
    parser.add_argument("--date", "-d", default=None, help="分析截止日期 YYYY-MM-DD (默认今天)")
    parser.add_argument("--days", "-n", type=int, default=20, help="回顾交易日数 (默认20)")
    parser.add_argument("--top", type=int, default=5, help="展示TOP N强势板块 (默认5)")
    parser.add_argument("--json", action="store_true", help="输出JSON格式")
    args = parser.parse_args()

    result = run_full_analysis(lookback_days=args.days, end_date=args.date, top_n=args.top)

    if "error" in result:
        print(result["error"])
        return

    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    else:
        print(format_report(result))


if __name__ == "__main__":
    main()
