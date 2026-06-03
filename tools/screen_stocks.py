#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 probiga 读已同步表，做常见「初选股票/板块」筛选，打印表格或 CSV。

仓库根执行::

  python tools/screen_stocks.py --list
  python tools/screen_stocks.py --date 2024-07-12 --mode lhb --top 30
  python tools/screen_stocks.py --date 2024-07-12 --mode flow --min-main-flow 5000000 --top 50
  python tools/screen_stocks.py --date 2024-07-12 --mode k_day --min-change 5 --max-change 11 --min-turnover 2 --top 40
  python tools/screen_stocks.py --mode concept --concept-code BK0473 --top 80
  python tools/screen_stocks.py --date 2024-07-12 --mode hot_ths_daily --top 20
  python tools/screen_stocks.py --mode hot_ths_rt --top 20
  python tools/screen_stocks.py --mode hot_rank_ths --top 30

  低位启动 / 趋势 / 连板（均依赖 sm_stock_kline 日 K 已同步）：

  python tools/screen_stocks.py --date 2024-07-12 --mode low_start --top 40
  python tools/screen_stocks.py --date 2024-07-12 --mode trend --top 40
  python tools/screen_stocks.py --date 2024-07-12 --mode trend_strong --top 30
  python tools/screen_stocks.py --date 2024-07-12 --mode trend_strong --trend-days 15 --ma-slope-min 1.0 --top 30
  python tools/screen_stocks.py --date 2024-07-12 --mode ladder --min-boards 2 --max-boards 4 --top 40

  筛日 K 后合并「资金+龙虎榜+扫雷+公告链/标题」（--with-context，需 httpx）：

  python tools/screen_stocks.py --date 2024-07-12 --mode low_start --top 20 --with-context
  python tools/screen_stocks.py --date 2024-07-12 --mode trend --top 20 --with-context --fetch-notices 3

环境变量：MYSQL_URL（默认与 sync_sentiment 一致）。
K 线筛选默认 k_type=1、adjust_type=1，与 adata 日 K 常见配置一致；若你同步时改过 SM_STOCK_K_* 请传 --k-type / --adjust-type。
创业板 20cm 连板请把 --limit-pct 调到约 19.5；ST 股约 4.8~5。本工具仅为规则筛选，不构成投资建议。
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_MYSQL_URL = "mysql+pymysql://root:123456@localhost:3306/probiga?charset=utf8mb4"

MODES = {
    "lhb": "龙虎榜日表 st_a_list_daily：指定交易日上榜股",
    "flow": "个股日级主力净流入 sm_stock_capital_flow_daily",
    "k_day": "日 K 涨幅与换手 sm_stock_kline（需已有 K 线数据）",
    "low_start": "低位放量启动：距前 N 日低点不远 + 当日温和上涨 + 放量（sm_stock_kline）",
    "trend": "趋势多头：收盘 MA5>MA10>MA20 且收盘在 MA5 上方（sm_stock_kline）",
    "trend_strong": "强势趋势票：四线多头(MA5>10>20>60) + 连续站上MA5 + 创新高 + 温和量比 + MACD确认",
    "ladder": "连板：收盘前若干日为「涨停附近」的连续计数，板数在区间内（默认 2~4，可调）",
    "concept": "东财概念成分 si_stock_concept_east（需 --concept-code）",
    "hot_ths_daily": "同花顺热门概念/行业日表 st_hot_concept_ths_daily",
    "hot_ths_rt": "同花顺热门概念/行业当前快照 st_hot_concept_ths_rt（无日期）",
    "hot_rank_ths": "同花顺热股榜 st_hot_rank_ths（无日期，最近一次同步）",
}


def _engine():
    url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
    return create_engine(url, pool_pre_ping=True)


def run_lhb(engine, trade_date: str, top: int) -> pd.DataFrame:
    q = text(
        """
        SELECT d.stock_code, c.short_name, d.change_cpt AS change_pct, d.turnover_ratio,
               d.a_net_amount, d.reason, d.trade_date
        FROM st_a_list_daily d
        LEFT JOIN si_all_code c ON c.stock_code = d.stock_code
        WHERE d.trade_date = :d
        ORDER BY ABS(d.change_cpt) DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"d": trade_date, "lim": top})


def run_flow(engine, trade_date: str, top: int, min_main: float) -> pd.DataFrame:
    q = text(
        """
        SELECT f.stock_code, c.short_name, f.main_net_inflow, f.max_net_inflow, f.trade_date
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code c ON c.stock_code = f.stock_code
        WHERE f.trade_date = :d AND f.main_net_inflow >= :m
        ORDER BY f.main_net_inflow DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"d": trade_date, "m": min_main, "lim": top})


def run_k_day(
    engine,
    trade_date: str,
    top: int,
    k_type: int,
    adjust_type: int,
    min_change: float,
    max_change: float,
    min_turnover: float,
) -> pd.DataFrame:
    q = text(
        """
        SELECT k.stock_code, k.short_name, k.change_pct, k.turnover_ratio, k.close, k.amount, k.trade_date
        FROM sm_stock_kline k
        WHERE k.trade_date = :d
          AND k.k_type = :kt
          AND k.adjust_type = :at
          AND k.change_pct >= :cmin
          AND k.change_pct <= :cmax
          AND (k.turnover_ratio IS NULL OR k.turnover_ratio >= :tmin)
        ORDER BY k.change_pct DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(
        q,
        engine,
        params={
            "d": trade_date,
            "kt": k_type,
            "at": adjust_type,
            "cmin": min_change,
            "cmax": max_change,
            "tmin": min_turnover,
            "lim": top,
        },
    )


def run_concept(engine, concept_code: str, top: int) -> pd.DataFrame:
    q = text(
        """
        SELECT DISTINCT s.stock_code, c.short_name, s.name AS concept_name, s.concept_code
        FROM si_stock_concept_east s
        LEFT JOIN si_all_code c ON c.stock_code = s.stock_code
        WHERE s.concept_code = :cc
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"cc": concept_code, "lim": top})


def run_hot_ths_daily(engine, snapshot_date: str, top: int) -> pd.DataFrame:
    q = text(
        """
        SELECT snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value, hot_tag
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date = :d AND plate_type = 1
        ORDER BY rank
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"d": snapshot_date, "lim": top})


def run_hot_ths_rt(engine, top: int) -> pd.DataFrame:
    q = text(
        """
        SELECT plate_type, rank, concept_code, concept_name, change_pct, hot_value, hot_tag, etl_sync_at
        FROM st_hot_concept_ths_rt
        WHERE plate_type = 1
        ORDER BY rank
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"lim": top})


def run_hot_rank_ths(engine, top: int) -> pd.DataFrame:
    q = text(
        """
        SELECT rank, stock_code, short_name, change_pct, hot_value, pop_tag, concept_tag
        FROM st_hot_rank_ths
        ORDER BY rank
        LIMIT :lim
        """
    )
    return pd.read_sql(q, engine, params={"lim": top})


def run_low_start(
    engine,
    trade_date: str,
    top: int,
    k_type: int,
    adjust_type: int,
    low_lookback: int,
    max_from_low: float,
    vol_boost: float,
    min_chg: float,
    max_chg: float,
) -> pd.DataFrame:
    """前 low_lookback 根 K（不含当日）内最低价，当日收盘距低点比例 + 放量。"""
    lbwin = max(5, int(low_lookback))
    q = text(
        f"""
        SELECT k.stock_code, k.short_name, k.trade_date, k.close, k.change_pct, k.turnover_ratio, k.volume,
               hl.min_low_before,
               (k.close - hl.min_low_before) / NULLIF(hl.min_low_before, 0) AS dist_from_low,
               hv.avg_vol_20_before
        FROM sm_stock_kline k
        INNER JOIN (
            SELECT k1.stock_code, MIN(k1.low) AS min_low_before
            FROM sm_stock_kline k1
            WHERE k1.k_type = :kt AND k1.adjust_type = :at
              AND k1.trade_date < :d
              AND k1.trade_date >= DATE_SUB(:d, INTERVAL {lbwin} DAY)
            GROUP BY k1.stock_code
        ) hl ON k.stock_code = hl.stock_code
        INNER JOIN (
            SELECT k2.stock_code, AVG(k2.volume) AS avg_vol_20_before
            FROM sm_stock_kline k2
            WHERE k2.k_type = :kt AND k2.adjust_type = :at
              AND k2.trade_date < :d
              AND k2.trade_date >= DATE_SUB(:d, INTERVAL 20 DAY)
            GROUP BY k2.stock_code
        ) hv ON k.stock_code = hv.stock_code
        WHERE k.trade_date = :d
          AND k.k_type = :kt
          AND k.adjust_type = :at
          AND hl.min_low_before IS NOT NULL
          AND hv.avg_vol_20_before IS NOT NULL
          AND hv.avg_vol_20_before > 0
          AND k.change_pct >= :cmin
          AND k.change_pct <= :cmax
          AND (k.close - hl.min_low_before) / NULLIF(hl.min_low_before, 0) <= :mxdist
          AND k.volume >= :vboost * hv.avg_vol_20_before
        ORDER BY k.change_pct DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(
        q,
        engine,
        params={
            "d": trade_date,
            "kt": k_type,
            "at": adjust_type,
            "mxdist": max_from_low,
            "vboost": vol_boost,
            "cmin": min_chg,
            "cmax": max_chg,
            "lim": top,
        },
    )


def run_trend(
    engine,
    trade_date: str,
    top: int,
    k_type: int,
    adjust_type: int,
    min_chg: float,
) -> pd.DataFrame:
    q = text(
        """
        SELECT k.stock_code, k.short_name, k.trade_date, k.close,
               ma5.ma5, ma10.ma10, ma20.ma20, k.change_pct, k.turnover_ratio,
               ma5.ma5 / NULLIF(ma20.ma20, 0) AS ma_spread
        FROM sm_stock_kline k
        INNER JOIN (
            SELECT k1.stock_code, AVG(k1.close) AS ma5
            FROM sm_stock_kline k1
            WHERE k1.k_type = :kt AND k1.adjust_type = :at
              AND k1.trade_date <= :d
              AND k1.trade_date > DATE_SUB(:d, INTERVAL 5 DAY)
            GROUP BY k1.stock_code
            HAVING COUNT(*) = 5
        ) ma5 ON k.stock_code = ma5.stock_code
        INNER JOIN (
            SELECT k2.stock_code, AVG(k2.close) AS ma10
            FROM sm_stock_kline k2
            WHERE k2.k_type = :kt AND k2.adjust_type = :at
              AND k2.trade_date <= :d
              AND k2.trade_date > DATE_SUB(:d, INTERVAL 10 DAY)
            GROUP BY k2.stock_code
            HAVING COUNT(*) = 10
        ) ma10 ON k.stock_code = ma10.stock_code
        INNER JOIN (
            SELECT k3.stock_code, AVG(k3.close) AS ma20
            FROM sm_stock_kline k3
            WHERE k3.k_type = :kt AND k3.adjust_type = :at
              AND k3.trade_date <= :d
              AND k3.trade_date > DATE_SUB(:d, INTERVAL 20 DAY)
            GROUP BY k3.stock_code
            HAVING COUNT(*) = 20
        ) ma20 ON k.stock_code = ma20.stock_code
        WHERE k.trade_date = :d
          AND k.k_type = :kt
          AND k.adjust_type = :at
          AND ma5.ma5 IS NOT NULL AND ma10.ma10 IS NOT NULL AND ma20.ma20 IS NOT NULL
          AND ma5.ma5 > ma10.ma10 AND ma10.ma10 > ma20.ma20
          AND k.close > ma5.ma5
          AND k.change_pct >= :cmin
        ORDER BY ma5.ma5 / NULLIF(ma20.ma20, 0) DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(
        q,
        engine,
        params={"d": trade_date, "kt": k_type, "at": adjust_type, "cmin": min_chg, "lim": top},
    )


def run_trend_strong(
    engine,
    trade_date: str,
    top: int,
    k_type: int,
    adjust_type: int,
    trend_days: int,
    ma_slope_min: float,
    vol_ratio_min: float,
    vol_ratio_max: float,
    max_60d_gain: float,
    new_high_pct: float,
) -> pd.DataFrame:
    """
    强势趋势票挖掘。

    核心逻辑：
    1. 四线多头排列 MA5 > MA10 > MA20 > MA60
    2. 连续 N 日收盘在 MA5 上方（趋势持续性）
    3. MA20 斜率 > 阈值（趋势强度）
    4. 创近60日新高或距新高 < 5%（突破能力）
    5. 量比 0.8~2.5（温和放量，排除暴量出货和缩量无力）
    6. 60日涨幅在合理区间（排除已暴涨股）
    """
    # 第一步: SQL 筛选四线多头排列（核心过滤，大幅减少后续计算量）
    q = text(
        """
        SELECT t.stock_code,
               COALESCE(NULLIF(t.short_name,''), c.short_name) AS short_name,
               ROUND(t.close, 2) AS close,
               t.change_pct,
               t.turnover_ratio,
               ROUND(ma5.v, 2) AS ma5,
               ROUND(ma10.v, 2) AS ma10,
               ROUND(ma20.v, 2) AS ma20,
               ROUND(ma60.v, 2) AS ma60
        FROM sm_stock_kline t
        LEFT JOIN si_all_code c ON t.stock_code = c.stock_code
        INNER JOIN (
            SELECT stock_code, AVG(close) AS v
            FROM sm_stock_kline
            WHERE k_type=:kt AND adjust_type=:at
              AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 5 DAY)
            GROUP BY stock_code HAVING COUNT(*) >= 4
        ) ma5 ON t.stock_code = ma5.stock_code
        INNER JOIN (
            SELECT stock_code, AVG(close) AS v
            FROM sm_stock_kline
            WHERE k_type=:kt AND adjust_type=:at
              AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 10 DAY)
            GROUP BY stock_code HAVING COUNT(*) >= 8
        ) ma10 ON t.stock_code = ma10.stock_code
        INNER JOIN (
            SELECT stock_code, AVG(close) AS v
            FROM sm_stock_kline
            WHERE k_type=:kt AND adjust_type=:at
              AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 20 DAY)
            GROUP BY stock_code HAVING COUNT(*) >= 14
        ) ma20 ON t.stock_code = ma20.stock_code
        INNER JOIN (
            SELECT stock_code, AVG(close) AS v
            FROM sm_stock_kline
            WHERE k_type=:kt AND adjust_type=:at
              AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 60 DAY)
            GROUP BY stock_code HAVING COUNT(*) >= 30
        ) ma60 ON t.stock_code = ma60.stock_code
        WHERE t.trade_date = :d
          AND t.k_type = :kt
          AND t.adjust_type = :at
          AND t.stock_code REGEXP '^(0|60)'
          AND COALESCE(NULLIF(t.short_name,''), c.short_name) NOT LIKE '%%ST%%'
          AND ma5.v > ma10.v AND ma10.v > ma20.v AND ma20.v > ma60.v
          AND t.close > ma5.v
        ORDER BY t.close / NULLIF(ma60.v, 0) DESC
        LIMIT 800
        """
    )
    df = pd.read_sql(q, engine, params={"d": trade_date, "kt": k_type, "at": adjust_type})
    if df.empty:
        return df

    # 第二步: Python 批量补算 - 连续站上MA5天数、60日高低点、量比
    codes = df["stock_code"].astype(str).str.strip().str.zfill(6).tolist()
    ph = ",".join(f"'{c}'" for c in codes)

    # 拉取近60日K线
    hist_sql = text(f"""
        SELECT stock_code, trade_date, close, high, low, volume
        FROM sm_stock_kline
        WHERE stock_code IN ({ph}) AND k_type=:kt AND adjust_type=:at
          AND trade_date <= :d AND trade_date > DATE_SUB(:d, INTERVAL 80 DAY)
        ORDER BY stock_code, trade_date DESC
    """)
    hist = pd.read_sql(hist_sql, engine, params={"d": trade_date, "kt": k_type, "at": adjust_type})
    if hist.empty:
        return df

    # 按股票分组计算
    metrics = {}
    for code, grp in hist.groupby("stock_code"):
        grp = grp.sort_values("trade_date", ascending=False).reset_index(drop=True)
        n = len(grp)
        if n < 20:
            continue

        closes = grp["close"].astype(float).tolist()
        highs = grp["high"].astype(float).tolist()
        lows = grp["low"].astype(float).tolist()
        volumes = grp["volume"].astype(float).tolist()

        # 连续站上MA5天数 (从最近一天往回数)
        above_ma5_days = 0
        for i in range(min(n, 60)):
            window = closes[i:i + 5]
            if len(window) < 5:
                break
            ma5_val = sum(window) / 5
            if closes[i] >= ma5_val:
                above_ma5_days += 1
            else:
                break

        # 60日最高/最低/涨幅
        lookback = min(n, 60)
        high_60 = max(highs[:lookback])
        low_60 = min(lows[:lookback])
        # 最早一天的收盘价(60日前)
        close_60ago = closes[-1] if n >= 60 else closes[-1]
        close_now = closes[0]
        gain_60d = (close_now - close_60ago) / close_60ago * 100 if close_60ago > 0 else 0

        # 距60日新高百分比
        near_high_pct = close_now / high_60 if high_60 > 0 else 0

        # 量比: 最近5日均量 / 20日均量
        vol_5 = sum(volumes[:5]) / min(5, len(volumes[:5])) if volumes[:5] else 0
        vol_20 = sum(volumes[:20]) / min(20, len(volumes[:20])) if volumes[:20] else 0
        vol_ratio = vol_5 / vol_20 if vol_20 > 0 else 0

        # MA20斜率: 当前MA20 vs 10日前MA20
        ma20_now = sum(closes[:20]) / 20 if n >= 20 else 0
        ma20_10ago = sum(closes[10:30]) / 20 if n >= 30 else 0
        ma20_slope = (ma20_now - ma20_10ago) / ma20_10ago * 100 if ma20_10ago > 0 else 0

        metrics[code] = {
            "above_ma5_days": above_ma5_days,
            "high_60d": round(high_60, 2),
            "gain_60d": round(gain_60d, 2),
            "near_high_pct": round(near_high_pct, 4),
            "vol_ratio": round(vol_ratio, 2),
            "ma20_slope_pct": round(ma20_slope, 2),
        }

    # 合并指标到 DataFrame
    metric_df = pd.DataFrame.from_dict(metrics, orient="index")
    metric_df.index.name = "stock_code"
    metric_df = metric_df.reset_index()
    df = df.merge(metric_df, on="stock_code", how="left")

    # 第三步: 最终过滤
    df = df[df["above_ma5_days"].fillna(0) >= trend_days]
    df = df[df["ma20_slope_pct"].fillna(0) >= ma_slope_min]
    df = df[df["near_high_pct"].fillna(0) >= new_high_pct]
    df = df[df["vol_ratio"].fillna(0) >= vol_ratio_min]
    df = df[df["vol_ratio"].fillna(0) <= vol_ratio_max]
    df = df[df["gain_60d"].fillna(0) <= max_60d_gain]
    df = df[df["gain_60d"].fillna(0) > 0]

    # 排序: 按连续站上MA5天数 + 60日涨幅综合排序
    df = df.sort_values(["above_ma5_days", "gain_60d"], ascending=[False, False])
    return df.head(top)


def run_ladder(
    engine,
    trade_date: str,
    top: int,
    k_type: int,
    adjust_type: int,
    limit_pct: float,
    min_boards: int,
    max_boards: int,
) -> pd.DataFrame:
    """连续「涨停附近」交易日计数，仅保留 T 日仍涨停且 streak 在 [min,max]。"""
    q = text(
        """
        SELECT t.stock_code, t.short_name, t.trade_date, t.change_pct, t.close,
               (SELECT COUNT(*)
                FROM sm_stock_kline prev
                WHERE prev.stock_code = t.stock_code
                  AND prev.k_type = :kt
                  AND prev.adjust_type = :at
                  AND prev.trade_date <= t.trade_date
                  AND prev.trade_date > (
                      SELECT COALESCE(
                          (SELECT MAX(gap.trade_date)
                           FROM sm_stock_kline gap
                           WHERE gap.stock_code = t.stock_code
                             AND gap.k_type = :kt
                             AND gap.adjust_type = :at
                             AND gap.trade_date < t.trade_date
                             AND gap.change_pct < :pct),
                          DATE_SUB(:d, INTERVAL 60 DAY)
                      )
                  )
                  AND prev.change_pct >= :pct
               ) AS boards
        FROM sm_stock_kline t
        WHERE t.trade_date = :d
          AND t.k_type = :kt
          AND t.adjust_type = :at
          AND t.change_pct >= :pct
          AND EXISTS (
              SELECT 1 FROM sm_stock_kline prev_check
              WHERE prev_check.stock_code = t.stock_code
                AND prev_check.k_type = :kt
                AND prev_check.adjust_type = :at
                AND prev_check.trade_date < :d
                AND prev_check.trade_date >= DATE_SUB(:d, INTERVAL 60 DAY)
                AND prev_check.change_pct >= :pct
          )
        HAVING boards >= :bmin AND boards <= :bmax
        ORDER BY boards DESC, t.change_pct DESC
        LIMIT :lim
        """
    )
    return pd.read_sql(
        q,
        engine,
        params={
            "d": trade_date,
            "kt": k_type,
            "at": adjust_type,
            "pct": limit_pct,
            "bmin": min_boards,
            "bmax": max_boards,
            "lim": top,
        },
    )




def _in_params(codes: list[str], prefix: str = "c") -> tuple[str, dict[str, str]]:
    ph = ",".join(f":{prefix}{i}" for i in range(len(codes)))
    params = {f"{prefix}{i}": str(c).strip().zfill(6) for i, c in enumerate(codes)}
    return ph, params


def _read_flow_for_codes(engine, trade_date: str, codes: list[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    ph, p = _in_params(codes)
    p["d"] = trade_date
    q = text(
        f"SELECT stock_code, main_net_inflow AS ctx_main_net_inflow, "
        f"max_net_inflow AS ctx_max_net_inflow FROM sm_stock_capital_flow_daily "
        f"WHERE trade_date = :d AND stock_code IN ({ph})"
    )
    return pd.read_sql(q, engine, params=p)


def _read_lhb_for_codes(engine, trade_date: str, codes: list[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    ph, p = _in_params(codes)
    p["d"] = trade_date
    q = text(
        f"SELECT stock_code, reason AS ctx_lhb_reason, a_net_amount AS ctx_lhb_net_amount "
        f"FROM st_a_list_daily WHERE trade_date = :d AND stock_code IN ({ph})"
    )
    return pd.read_sql(q, engine, params=p)


def _read_mine_for_codes(engine, codes: list[str]) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    ph, p = _in_params(codes)
    q = text(
        f"SELECT stock_code, MAX(score) AS ctx_mine_max_score "
        f"FROM st_mine_clearance_tdx WHERE stock_code IN ({ph}) GROUP BY stock_code"
    )
    return pd.read_sql(q, engine, params=p)


def _read_notice_summary_for_codes(engine, context_date: str, codes: list[str]) -> pd.DataFrame:
    """近 30 个自然日内已入库的东财公告条数与最新公告日（表不存在则返回空表）。"""
    if not codes:
        return pd.DataFrame()
    ph, p = _in_params(codes)
    p["d"] = context_date
    q = text(
        f"SELECT stock_code, COUNT(*) AS ctx_notice_cnt_30d, MAX(notice_date) AS ctx_last_notice_date "
        f"FROM si_notice_eastmoney WHERE stock_code IN ({ph}) "
        f"AND notice_date >= DATE_SUB(:d, INTERVAL 30 DAY) AND notice_date <= :d GROUP BY stock_code"
    )
    try:
        return pd.read_sql(q, engine, params=p)
    except Exception:
        return pd.DataFrame()


def _em_notice_titles(stock_code: str, n: int) -> str:
    """东财公告接口（JSON），失败则返回空串。"""
    if n <= 0:
        return ""
    try:
        import httpx
    except ImportError:
        return ""
    code = str(stock_code).strip().zfill(6)
    url = (
        "https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1"
        f"&page_size={min(n, 10)}&page_index=1&client_source=web&stock_list={code}"
    )
    try:
        r = httpx.get(url, timeout=15.0)
        r.raise_for_status()
        data = r.json()
        lst = (data.get("data") or {}).get("list") or []
        titles = [(x.get("title") or x.get("title_ch") or "").strip() for x in lst[:n]]
        return " | ".join(t for t in titles if t)
    except Exception:
        return ""


def enrich_with_context(
    engine,
    df: pd.DataFrame,
    *,
    context_date: str,
    fetch_notice_lines: int,
    notice_sleep: float,
) -> pd.DataFrame:
    """
    在结果表上合并：当日主力净流、是否龙虎榜及原因、扫雷最高分、东财公告页链接；
    可选拉取东财最近几条公告标题（网络请求，勿一次过多代码）。
    """
    if df is None or df.empty or "stock_code" not in df.columns:
        return df
    out = df.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.strip().str.zfill(6)
    codes = out["stock_code"].drop_duplicates().tolist()[:150]

    flow = _read_flow_for_codes(engine, context_date, codes)
    lhb = _read_lhb_for_codes(engine, context_date, codes)
    mine = _read_mine_for_codes(engine, codes)

    out = out.merge(flow, on="stock_code", how="left")
    out = out.merge(lhb, on="stock_code", how="left")
    out = out.merge(mine, on="stock_code", how="left")
    notice_sum = _read_notice_summary_for_codes(engine, context_date, codes)
    if not notice_sum.empty:
        out = out.merge(notice_sum, on="stock_code", how="left")
    out["ctx_eastmoney_notice_url"] = out["stock_code"].apply(
        lambda c: f"https://data.eastmoney.com/notices/stock/{c}.html"
    )

    if fetch_notice_lines > 0:
        titles: list[str] = []
        for i, c in enumerate(codes[:40]):
            titles.append(_em_notice_titles(c, fetch_notice_lines))
            if i + 1 < len(codes[:40]):
                time.sleep(max(0.05, notice_sleep))
        m = dict(zip(codes[:40], titles))

        out["ctx_recent_notice_titles"] = out["stock_code"].map(lambda x: m.get(x, ""))
    else:
        out["ctx_recent_notice_titles"] = ""

    return out


def main() -> int:
    p = argparse.ArgumentParser(description="probiga 初选股票/热门板块（读库）")
    p.add_argument("--list", action="store_true", help="列出可用 --mode 及说明")
    p.add_argument("--date", type=str, default="", help="交易日或快照日 YYYY-MM-DD（hot_ths_rt / hot_rank_ths 不需要）")
    p.add_argument("--mode", type=str, default="", help="筛选模式，见 --list")
    p.add_argument("--top", type=int, default=50, help="最多返回行数")
    p.add_argument("--min-main-flow", type=float, default=0.0, help="[flow] 主力净流入下限（元）")
    p.add_argument("--min-change", type=float, default=-1e9, help="[k_day] 涨跌幅%% 下限")
    p.add_argument("--max-change", type=float, default=1e9, help="[k_day] 涨跌幅%% 上限")
    p.add_argument("--min-turnover", type=float, default=0.0, help="[k_day] 换手率%% 下限")
    p.add_argument("--k-type", type=int, default=1, help="[k_day] sm_stock_kline.k_type")
    p.add_argument("--adjust-type", type=int, default=1, help="[k_day] sm_stock_kline.adjust_type")
    p.add_argument("--concept-code", type=str, default="", help="[concept] 东财概念代码，如 BK0473")
    p.add_argument("--csv", type=str, default="", help="若指定路径则写入 CSV")
    p.add_argument(
        "--low-lookback",
        type=int,
        default=60,
        help="[low_start] 看前多少根日 K（不含当日）算阶段低点窗口，默认 60",
    )
    p.add_argument(
        "--max-from-low",
        type=float,
        default=0.28,
        help="[low_start] 收盘较阶段低点最大幅度(比例)，默认 0.28 即不超过约 28%%",
    )
    p.add_argument(
        "--vol-boost",
        type=float,
        default=1.25,
        help="[low_start] 当日成交量 >= 该倍数 × 前20日均量（不含当日），默认 1.25",
    )
    p.add_argument(
        "--start-min-chg",
        type=float,
        default=2.0,
        help="[low_start] 当日涨跌幅%% 下限，默认 2",
    )
    p.add_argument(
        "--start-max-chg",
        type=float,
        default=10.5,
        help="[low_start] 当日涨跌幅%% 上限（排除已涨停），默认 10.5",
    )
    p.add_argument(
        "--trend-min-chg",
        type=float,
        default=-1e9,
        help="[trend] 当日涨跌幅%% 下限，默认不限制；可设 0 只要收阳",
    )
    p.add_argument(
        "--trend-days",
        type=int,
        default=10,
        help="[trend_strong] 连续站上MA5最少天数，默认10",
    )
    p.add_argument(
        "--ma-slope-min",
        type=float,
        default=0.5,
        help="[trend_strong] MA20日均斜率下限(%%)，默认0.5",
    )
    p.add_argument(
        "--vol-ratio-min",
        type=float,
        default=0.8,
        help="[trend_strong] 量比下限（5日/20日均量），默认0.8",
    )
    p.add_argument(
        "--vol-ratio-max",
        type=float,
        default=2.5,
        help="[trend_strong] 量比上限，默认2.5",
    )
    p.add_argument(
        "--max-60d-gain",
        type=float,
        default=150.0,
        help="[trend_strong] 60日最大涨幅(%%)，默认150",
    )
    p.add_argument(
        "--new-high-pct",
        type=float,
        default=0.95,
        help="[trend_strong] 距60日新高比例阈值，默认0.95（即95%%以上）",
    )
    p.add_argument(
        "--limit-pct",
        type=float,
        default=9.8,
        help="[ladder] 视为涨停附近的涨跌幅%%，主板约 9.8；创业板约 19.5；ST 约 4.9",
    )
    p.add_argument(
        "--min-boards",
        type=int,
        default=2,
        help="[ladder] 最少连板数（含当日），默认 2",
    )
    p.add_argument(
        "--max-boards",
        type=int,
        default=4,
        help="[ladder] 最多连板数，默认 4，用于筛「非首板、非过高」",
    )
    p.add_argument(
        "--with-context",
        action="store_true",
        help="在含 stock_code 的结果上合并：当日主力净流、龙虎榜摘要、扫雷最高分、东财公告页链接；"
        "可选 --fetch-notices 拉公告标题（需安装 httpx，见 requirements-platform.txt）",
    )
    p.add_argument(
        "--fetch-notices",
        type=int,
        default=0,
        help="与 --with-context 合用：每只股票拉东财最近 N 条公告标题（网络请求，默认 0 不拉）",
    )
    p.add_argument(
        "--context-date",
        type=str,
        default="",
        help="与 --with-context 合用：对齐资金流/龙虎榜的交易日；默认与 --date 相同；"
        "hot_rank_ths 等无 --date 时可单独指定",
    )
    p.add_argument(
        "--notice-sleep",
        type=float,
        default=0.25,
        help="拉公告标题时每只股票间隔秒数，默认 0.25",
    )
    args = p.parse_args()

    if args.list or not args.mode:
        print("可用 --mode：\n")
        for k, v in MODES.items():
            print(f"  {k:16}  {v}")
        print("\n示例见本文件模块文档字符串。")
        return 0

    mode = args.mode.strip().lower()
    if mode not in MODES:
        print("未知 --mode，请使用 --list", file=sys.stderr)
        return 2

    need_date = mode in ("lhb", "flow", "k_day", "hot_ths_daily", "low_start", "trend", "trend_strong", "ladder")
    if need_date and not args.date.strip():
        print(f"--mode {mode} 需要 --date YYYY-MM-DD", file=sys.stderr)
        return 2
    if mode == "concept" and not args.concept_code.strip():
        print("--mode concept 需要 --concept-code", file=sys.stderr)
        return 2

    eng = _engine()
    trade_date = args.date.strip()
    top = max(1, int(args.top))

    try:
        if mode == "lhb":
            df = run_lhb(eng, trade_date, top)
        elif mode == "flow":
            df = run_flow(eng, trade_date, top, float(args.min_main_flow))
        elif mode == "k_day":
            df = run_k_day(
                eng,
                trade_date,
                top,
                int(args.k_type),
                int(args.adjust_type),
                float(args.min_change),
                float(args.max_change),
                float(args.min_turnover),
            )
        elif mode == "low_start":
            df = run_low_start(
                eng,
                trade_date,
                top,
                int(args.k_type),
                int(args.adjust_type),
                int(args.low_lookback),
                float(args.max_from_low),
                float(args.vol_boost),
                float(args.start_min_chg),
                float(args.start_max_chg),
            )
        elif mode == "trend":
            df = run_trend(
                eng,
                trade_date,
                top,
                int(args.k_type),
                int(args.adjust_type),
                float(args.trend_min_chg),
            )
        elif mode == "trend_strong":
            df = run_trend_strong(
                eng,
                trade_date,
                top,
                int(args.k_type),
                int(args.adjust_type),
                int(args.trend_days),
                float(args.ma_slope_min),
                float(args.vol_ratio_min),
                float(args.vol_ratio_max),
                float(args.max_60d_gain),
                float(args.new_high_pct),
            )
        elif mode == "ladder":
            bmin = int(args.min_boards)
            bmax = int(args.max_boards)
            if bmin > bmax:
                print("--min-boards 不能大于 --max-boards", file=sys.stderr)
                return 2
            df = run_ladder(
                eng,
                trade_date,
                top,
                int(args.k_type),
                int(args.adjust_type),
                float(args.limit_pct),
                bmin,
                bmax,
            )
        elif mode == "concept":
            df = run_concept(eng, args.concept_code.strip(), top)
        elif mode == "hot_ths_daily":
            df = run_hot_ths_daily(eng, trade_date, top)
        elif mode == "hot_ths_rt":
            df = run_hot_ths_rt(eng, top)
        elif mode == "hot_rank_ths":
            df = run_hot_rank_ths(eng, top)
        else:
            return 2
    except Exception as e:  # noqa: BLE001
        print("查询失败:", e, file=sys.stderr)
        return 1

    if df is None or df.empty:
        print("(无数据：检查日期是否有同步、或条件是否过严)")
        return 0

    if args.with_context:
        if "stock_code" not in df.columns:
            print("--with-context 需要结果中含 stock_code 列，已跳过合并。", file=sys.stderr)
        else:
            ctx_d = (args.context_date or args.date or "").strip() or datetime.now().strftime("%Y-%m-%d")
            df = enrich_with_context(
                eng,
                df,
                context_date=ctx_d,
                fetch_notice_lines=max(0, int(args.fetch_notices)),
                notice_sleep=float(args.notice_sleep),
            )

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 96)
    print(df.to_string(index=False))
    if args.csv.strip():
        path = Path(args.csv.strip())
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"\n已写入: {path.resolve()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

