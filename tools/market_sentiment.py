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

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

INDEX_MAP = {
    "000016": "上证50",
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "399303": "国证2000",
    "399006": "创业板指",
    "000688": "科创50",
}


def _engine():
    url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
    return create_engine(url, pool_pre_ping=True)


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
        return {"status": "no_data", "message": "概念热度数据为空"}

    concept_df = df[df["plate_type"] == 1].copy()
    industry_df = df[df["plate_type"] == 2].copy()

    # 1.2 概念 — 主线识别
    concept_stats = _calc_theme_stats(concept_df, trade_dates, "概念")
    industry_stats = _calc_theme_stats(industry_df, trade_dates, "行业")

    # 1.3 综合评估轮动强度
    combined = concept_df if not concept_df.empty else industry_df

    # 轮动分数: 基于排名换手率
    rotation_score = _calc_rotation_score(concept_df, trade_dates)

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
    if rotation_score < 30 and len(main_themes) >= 2:
        phase = "主线行情"
        phase_desc = f"市场存在较明确的主线，近{n_days}日'{main_themes[0]['name']}'等板块持续强势"
    elif rotation_score < 50:
        phase = "弱主线轮动"
        phase_desc = f"部分板块略占优势，但轮动较快，缺乏持续主线"
    elif rotation_score < 70:
        phase = "快速轮动"
        phase_desc = f"板块切换频繁，热点难以持续2天以上，适合短线低吸"
    else:
        phase = "极端轮动/混沌"
        phase_desc = f"板块毫无持续性，市场方向不明，建议观望或极轻仓"

    return {
        "status": "ok",
        "lookback_days": n_days,
        "date_range": [d1, d2],
        "phase": phase,
        "phase_desc": phase_desc,
        "rotation_score": round(rotation_score, 1),
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


def _calc_rotation_score(df: pd.DataFrame, trade_dates: list[str]) -> float:
    """
    轮动强度 0-100:
      0 = 完全不轮动(同一板块始终霸榜)
      100 = 极致轮动(每天TOP10完全换血)
    """
    if df.empty or len(trade_dates) < 2:
        return 50.0

    df = _normalize_date_col(df, "snapshot_date")
    dates_sorted = sorted(df["_datestr"].unique().tolist())

    # 计算相邻日TOP10的Jaccard差异
    similarities = []
    new_entry_rates = []
    stable_ratios = []
    rank_volatilities = []

    for i in range(1, len(dates_sorted)):
        d_prev, d_curr = dates_sorted[i - 1], dates_sorted[i]
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
        return 50.0

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
      - 用全市场个股成交额作为市值代理, 拆分大盘/小盘
      - 对比大盘股 vs 小盘股每日涨跌表现
    """
    if not trade_dates:
        return {"status": "no_data", "message": "无交易日数据"}

    d1 = trade_dates[0]
    d2 = trade_dates[-1]

    # 2.1 加载涨跌+成交额数据，在Python端按成交额分大盘/小盘（兼容MySQL 5.7）
    raw_q = text("""
        SELECT trade_date, change_pct, amount
        FROM sm_stock_kline
        WHERE trade_date >= :d1 AND trade_date <= :d2 AND k_type = 1
          AND amount > 0
        ORDER BY trade_date, amount
    """)
    raw_df = pd.read_sql(raw_q, engine, params={"d1": d1, "d2": d2})

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
            daily_style[td_str] = {
                "avg_chg": round(float(avg_chg or 0), 4),
                "up_ratio": round(float(up_ratio or 0), 4),
                "large_chg": round(float(large['change_pct'].mean() or 0), 4),
                "large_cnt": int(len(large)),
                "small_chg": round(float(small['change_pct'].mean() or 0), 4),
                "small_cnt": int(len(small)),
            }
            market_df_rows.append({"trade_date": td_str, "avg_chg": avg_chg, "up_ratio": up_ratio})
    style_df = pd.DataFrame(market_df_rows) if market_df_rows else pd.DataFrame()

    daily_style = {}
    market_df_rows = []
    for _, row in style_df.iterrows():
        td_str = _to_date_str(row["trade_date"])
        daily_style[td_str] = {
            "avg_chg": round(float(row["avg_chg"] or 0), 4),
            "up_ratio": round(float(row["up_ratio"] or 0), 4),
            "large_chg": round(float(row["large_chg"] or 0), 4),
            "large_cnt": int(row["large_cnt"] or 0),
            "small_chg": round(float(row["small_chg"] or 0), 4),
            "small_cnt": int(row["small_cnt"] or 0),
            "mid_chg": 0,
            "mid_cnt": 0,
        }
        market_df_rows.append({"trade_date": row["trade_date"], "avg_chg": row["avg_chg"], "up_ratio": row["up_ratio"]})

    market_df = pd.DataFrame(market_df_rows)

    # 2.3 大小盘合计
    large_total_chg = 0
    small_total_chg = 0
    large_days = 0
    small_days = 0
    recent_large = 0
    recent_small = 0
    half_point = len(trade_dates) // 2

    for i, td in enumerate(trade_dates):
        v = daily_style.get(td, {})
        lc = v.get("large_chg", 0) or 0
        sc = v.get("small_chg", 0) or 0
        if lc != 0:
            large_total_chg += lc
            large_days += 1
        if sc != 0:
            small_total_chg += sc
            small_days += 1
        if i >= half_point:
            recent_large += lc
            recent_small += sc

    large_avg_daily = round(large_total_chg / large_days, 2) if large_days > 0 else 0
    small_avg_daily = round(small_total_chg / small_days, 2) if small_days > 0 else 0
    diff = round(small_avg_daily - large_avg_daily, 2)

    # 近期趋势
    recent_large_avg = round(recent_large / max(len(trade_dates) - half_point, 1), 2)
    recent_small_avg = round(recent_small / max(len(trade_dates) - half_point, 1), 2)

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

    try:
        idx_q = text("""
            SELECT index_code, trade_date, close, change_pct
            FROM sm_index_kline
            WHERE index_code IN :codes AND trade_date >= :d1 AND trade_date <= :d2 AND k_type = 1
            ORDER BY index_code, trade_date
        """)
        idx_df = pd.read_sql(idx_q, engine, params={"codes": tuple(all_idx), "d1": d1, "d2": d2})
    except Exception:
        idx_df = pd.DataFrame()

    if idx_df.empty:
        try:
            current_q = text("""
                SELECT index_code, trade_date, price AS close, change_pct
                FROM sm_index_current
                WHERE index_code IN :codes
            """)
            idx_df = pd.read_sql(current_q, engine, params={"codes": tuple(all_idx)})
        except Exception:
            idx_df = pd.DataFrame()

    if idx_df.empty:
        # 没有指数K线, 用大小盘统计数据构建虚拟指数条目
        large_name = "大盘股(成交额Top30%)"
        small_name = "小盘股(成交额Bot30%)"
        indices = [
            {
                "code": "large_proxy", "name": large_name, "category": "大",
                "total_change_pct": large_avg_daily * len(trade_dates),
                "avg_daily_chg": large_avg_daily, "win_rate": 50,
                "half1_change_pct": 0, "half2_change_pct": 0,
                "momentum": large_momentum,
                "last_price": 0, "last_change_pct": recent_large_avg,
            },
            {
                "code": "small_proxy", "name": small_name, "category": "小",
                "total_change_pct": small_avg_daily * len(trade_dates),
                "avg_daily_chg": small_avg_daily, "win_rate": 50,
                "half1_change_pct": 0, "half2_change_pct": 0,
                "momentum": small_momentum,
                "last_price": 0, "last_change_pct": recent_small_avg,
            },
        ]
    else:
        for code in all_idx:
            sub = idx_df[idx_df["index_code"] == code]
            if sub.empty or len(sub) < 2:
                continue
            name = INDEX_MAP.get(code, code)
            sub = sub.sort_values("trade_date")
            first_close = float(sub["close"].iloc[0])
            last_close = float(sub["close"].iloc[-1])
            total_chg = (last_close - first_close) / first_close * 100 if first_close else 0
            avg_daily_chg = float(sub["change_pct"].mean())
            win_rate = float((sub["change_pct"] > 0).mean() * 100)

            half = len(sub) // 2
            first_half = sub.iloc[:half] if half > 0 else sub.iloc[:1]
            second_half = sub.iloc[half:]
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
                "avg_daily_chg": round(avg_daily_chg, 2),
                "win_rate": round(win_rate, 1),
                "half1_change_pct": round(h1_chg, 2),
                "half2_change_pct": round(h2_chg, 2),
                "momentum": momentum,
                "last_price": round(last_close, 2),
                "last_change_pct": round(float(sub["change_pct"].iloc[-1]), 2) if len(sub) > 0 else 0,
            })

    # 2.6 市场整体活跃度
    market_activity = {}
    if not market_df.empty:
        market_df_sorted = market_df.sort_values("trade_date")
        recent = market_df_sorted.tail(3)
        market_activity = {
            "recent_avg_chg": round(float(recent["avg_chg"].dropna().mean()) if not recent["avg_chg"].dropna().empty else 0, 2),
            "recent_up_ratio": round(float(recent["up_ratio"].dropna().mean()) * 100 if not recent["up_ratio"].dropna().empty else 50, 1),
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
               COUNT(*) AS stock_cnt,
               SUM(CASE WHEN main_net_inflow > 0 THEN 1 ELSE 0 END) AS inflow_cnt
        FROM sm_stock_capital_flow_daily
        WHERE trade_date >= :d1 AND trade_date <= :d2
        GROUP BY trade_date
        ORDER BY trade_date
    """)
    flow_df = pd.read_sql(flow_q, engine, params={"d1": d1, "d2": d2})

    if flow_df.empty:
        return {"status": "no_data", "message": "资金流向数据为空"}

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
    else:
        flow_style = "资金小幅净流出"

    if recent_flow > 0 and total_flow > 0:
        recent_trend = "近期资金持续流入"
    elif recent_flow < 0 and total_flow < 0:
        recent_trend = "近期资金持续流出"
    elif recent_flow > total_flow * 0.3:
        recent_trend = "近期资金边际改善"
    else:
        recent_trend = "近期资金边际走弱"

    # 北向资金
    north_total = 0
    north_text = ""
    try:
        north_q = text("""
            SELECT trade_date, net_flow
            FROM st_north_flow_daily
            WHERE trade_date >= :d1 AND trade_date <= :d2
            ORDER BY trade_date
        """)
        north_df = pd.read_sql(north_q, engine, params={"d1": d1, "d2": d2})
        north_total = float(north_df["net_flow"].sum()) if not north_df.empty and not north_df["net_flow"].isna().all() else 0
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
    except Exception:
        pass

    return {
        "status": "ok",
        "flow_style": flow_style,
        "recent_trend": recent_trend,
        "north_flow_note": north_text,
        "total_main_flow": round(total_flow, 0),
        "avg_daily_flow": round(avg_daily_flow, 0),
        "inflow_ratio": round(inflow_ratio * 100, 1),
        "north_total_flow": round(north_total, 0),
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
                    f"{t['appear_days']}/{result['lookback_days']:<7}"
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
        lines.append(f"  大小盘差值: {style['large_small_diff']:+.1f}% (正值=小盘强)")
        lines.append(f"  解读: {style['bias_desc']}")
        lines.append("")

        indices = style.get("indices", [])
        if indices:
            lines.append(f"  各指数近期表现:")
            lines.append(f"  {'指数':<12}{'类别':<6}{'区间涨跌':<12}{'胜率':<8}{'趋势':<10}{'最新价':<10}{'当日涨跌'}")
            lines.append(f"  {'-' * 68}")
            for idx in indices:
                lines.append(
                    f"  {idx['name']:<12}{idx['category']:<6}"
                    f"{idx['total_change_pct']:+.2f}%{'':>4}"
                    f"{idx['win_rate']:.1f}%{'':>4}"
                    f"{idx['momentum']:<10}"
                    f"{idx['last_price']:<10}"
                    f"{idx['last_change_pct']:+.2f}%"
                )
        else:
            lines.append(f"  (指数K线数据暂不可用，大小盘判断基于市场整体数据)")

        activity = style.get("market_activity", {})
        if activity:
            lines.append("")
            lines.append(f"  近3日市场活跃度: 均涨幅 {activity.get('recent_avg_chg', 0):+.2f}%  "
                         f"上涨比 {activity.get('recent_up_ratio', 0):.1f}%")

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
            lines.append(f"  {north}: {capital.get('north_total_flow', 0):,.0f}")

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
