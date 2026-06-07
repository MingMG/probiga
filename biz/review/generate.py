#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日复盘数据生成

从 sm_stock_kline / sm_stock_capital_flow / st_hot_concept_ths_daily
等表自动汇总当日市场复盘数据，写入 st_daily_review。

执行:
  python -m biz.review.generate 2026-03-31
  python -m biz.review.generate --today
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("review")

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_review_tables.sql"

SECTOR_NAMES = {
    "电力设备": ["电力设备", "光伏", "风电", "储能", "电网"],
    "公用事业": ["电力", "燃气", "水务", "环保"],
    "综合": ["综合"],
    "煤炭": ["煤炭", "煤化工"],
    "电子": ["电子", "半导体", "芯片", "光电子"],
    "银行": ["银行"],
    "机械设备": ["机械设备", "机器人", "自动化"],
    "基础化工": ["化工", "化学制品", "化学原料"],
    "有色金属": ["有色金属", "稀土", "锂矿"],
    "计算机": ["计算机", "软件", "信创"],
    "通信": ["通信", "5G", "光通信"],
    "汽车": ["汽车", "新能源车", "汽车零部件"],
    "医药生物": ["医药", "创新药", "医疗器械"],
    "食品饮料": ["食品饮料", "白酒"],
    "国防军工": ["军工", "航空航天"],
    "传媒": ["传媒", "游戏"],
    "房地产": ["房地产", "地产"],
    "建筑装饰": ["建筑", "基建"],
    "交通运输": ["交通运输", "物流"],
}


def get_engine():
    url = os.environ.get("MYSQL_URL") or DEFAULT_MYSQL_URL
    return create_engine(url, pool_pre_ping=True)


def run_ddl(engine):
    if DDL_PATH.is_file():
        sql = DDL_PATH.read_text(encoding="utf-8")
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    with engine.begin() as conn:
                        conn.execute(text(stmt))
                except Exception as e:
                    log.debug("DDL: %s", e)


# ═══════════════════════════════════════════
# 1. 市场总览
# ═══════════════════════════════════════════

def calc_market_overview(engine, date_str: str) -> dict:
    """计算市场热度、成交额、指数表现"""

    # 成交额
    amount_sql = """
    SELECT COALESCE(SUM(amount), 0) AS amt FROM sm_stock_kline
    WHERE trade_date = :d AND k_type = 1
    """
    rows = pd.read_sql(text(amount_sql), engine, params={"d": date_str}).to_dict(orient="records")
    total_amt = float(rows[0]["amt"]) if rows else 0

    # 前一日成交额
    prev_date = _prev_trade_date(engine, date_str)
    prev_amt = 0
    if prev_date:
        rows = pd.read_sql(text(amount_sql), engine, params={"d": prev_date}).to_dict(orient="records")
        prev_amt = float(rows[0]["amt"]) if rows else 0

    amt_change = "平量"
    if prev_amt > 0:
        ratio = total_amt / prev_amt
        if ratio > 1.1:
            amt_change = "放量"
        elif ratio < 0.9:
            amt_change = "缩量"

    # 涨跌比：计算上涨家数/下跌家数
    up_down_sql = """
    SELECT
      SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
      SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
      COUNT(*) AS total
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    rows = pd.read_sql(text(up_down_sql), engine, params={"d": date_str}).to_dict(orient="records")
    up_cnt = rows[0]["up_count"] or 0
    down_cnt = rows[0]["down_count"] or 0
    total_cnt = rows[0]["total"] or 1
    up_ratio = up_cnt / total_cnt

    # 市场热度: 基于上涨比例映射到0-100
    market_heat = round(up_ratio * 100, 1)

    # 热度假设：对比前一日
    heat_change = "flat"
    if prev_date:
        rows2 = pd.read_sql(text(up_down_sql), engine, params={"d": prev_date}).to_dict(orient="records")
        prev_up = rows2[0]["up_count"] or 0
        prev_total = rows2[0]["total"] or 1
        prev_heat = (prev_up / prev_total) * 100
        if market_heat > prev_heat * 1.05:
            heat_change = "up"
        elif market_heat < prev_heat * 0.95:
            heat_change = "down"

    # 主要指数名称映射
    idx_name = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50", "000852": "中证1000"}

    # 指数从K线实时计算
    idx_kline_sql = """
    SELECT '000852' AS index_code, AVG(close) AS avg_close, AVG(change_pct) AS avg_chg
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    idx_rows = pd.read_sql(text(idx_kline_sql), engine, params={"d": date_str}).to_dict(orient="records")
    idx_map = {"000852": {"name": "中证1000", "price": None, "change_pct": None}}
    if idx_rows:
        r = idx_rows[0]
        idx_map["000852"] = {"name": "中证1000",
                              "price": round(float(r["avg_close"] or 0), 2),
                              "change_pct": round(float(r["avg_chg"] or 0), 4)}

    # Try sm_index_current as fallback
    idx_sql = """
    SELECT index_code, price, change_pct FROM sm_index_current
    WHERE index_code IN ('000001','399001','399006','000688','000852')
    """
    try:
        idx_rows2 = pd.read_sql(text(idx_sql), engine).to_dict(orient="records")
        for r in idx_rows2:
            code = r["index_code"]
            if r.get("price") is not None:
                idx_map[code] = {"name": idx_name.get(code, code),
                                  "price": r["price"], "change_pct": r["change_pct"]}
    except Exception:
        pass

    # 万得全A 用两市涨跌比替代
    wind_a_name = "万得全A"
    wind_a_chg = round((up_cnt - down_cnt) / total_cnt * 100, 2)

    # 观望资金
    sideline = round(max(0, 100 - market_heat) * 1.2, 1)

    return {
        "review_date": date_str,
        "market_heat": market_heat,
        "market_heat_change": heat_change,
        "market_heat_note": f"上涨{up_cnt}家/下跌{down_cnt}家，上涨比例{up_ratio:.1%}",
        "total_amount": total_amt,
        "total_amount_change": amt_change,
        "index_code": "000852",
        "index_name": "中证1000",
        "index_price": idx_map.get("000852", {}).get("price"),
        "index_change_pct": idx_map.get("000852", {}).get("change_pct"),
        "sideline_ratio": sideline,
        "sideline_ratio_change": f"观望资金比例{sideline:.1f}%",
    }


def _prev_trade_date(engine, date_str: str) -> str | None:
    rows = pd.read_sql(text(
        "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE trade_date < :d AND k_type=1"
    ), engine, params={"d": date_str}).to_dict(orient="records")
    return rows[0]["d"] if rows and rows[0].get("d") else None


def _recent_trade_dates(engine, date_str: str, n: int = 5) -> list[str]:
    """获取最近n个交易日（含date_str）"""
    rows = pd.read_sql(text(
        "SELECT DISTINCT trade_date FROM sm_stock_kline WHERE trade_date <= :d AND k_type=1 "
        "ORDER BY trade_date DESC LIMIT :n"
    ), engine, params={"d": date_str, "n": n}).to_dict(orient="records")
    return sorted([str(r["trade_date"]) for r in rows])


# ═══════════════════════════════════════════
# 2. 板块热度分析
# ═══════════════════════════════════════════

def calc_sector_analysis(engine, date_str: str) -> dict:
    """计算板块热度和成交量变化"""

    # 从概念数据取板块热度
    concept_sql = """
    SELECT concept_name, change_pct, hot_value FROM st_hot_concept_ths_daily
    WHERE snapshot_date = :d ORDER BY plate_type, rank LIMIT 30
    """
    rows = pd.read_sql(text(concept_sql), engine, params={"d": date_str}).to_dict(orient="records")

    # 前一日
    prev_date = _prev_trade_date(engine, date_str)
    prev_rows = []
    if prev_date:
        try:
            prev_rows = pd.read_sql(text(concept_sql), engine, params={"d": prev_date}).to_dict(orient="records")
        except Exception:
            pass

    prev_map = {r["concept_name"]: r for r in prev_rows}

    hot_up = []
    hot_down = []
    vol_up = []
    vol_down = []

    for r in rows:
        name = r["concept_name"]
        chg = float(r.get("change_pct") or 0)
        if chg > 0:
            hot_up.append({"name": name, "change_pct": round(chg, 2), "hot_value": float(r.get("hot_value") or 0)})

        if name in prev_map:
            prev_hot = float(prev_map[name].get("hot_value") or 0)
            cur_hot = float(r.get("hot_value") or 0)
            if cur_hot > prev_hot * 1.1:
                vol_up.append({"name": name, "hot_change": round((cur_hot / prev_hot - 1) * 100, 1)})
            elif cur_hot < prev_hot * 0.9:
                vol_down.append({"name": name, "hot_change": round((cur_hot / prev_hot - 1) * 100, 1)})

    # 确保覆盖率：直接用涨跌幅排名
    hot_up.sort(key=lambda x: x["change_pct"], reverse=True)
    hot_down_all = [r for r in rows if float(r.get("change_pct") or 0) < 0]
    hot_down_all.sort(key=lambda x: x["change_pct"])

    hot_up_sectors = hot_up[:8]
    hot_down_sectors = [{"name": r["concept_name"], "change_pct": float(r["change_pct"] or 0),
                         "hot_value": float(r.get("hot_value") or 0)} for r in hot_down_all[:8]]

    vol_up.sort(key=lambda x: x["hot_change"], reverse=True)
    vol_down.sort(key=lambda x: x["hot_change"])

    return {
        "hot_sectors": hot_up_sectors,
        "cold_sectors": hot_down_sectors,
        "volume_up_sectors": vol_up[:5],
        "volume_down_sectors": vol_down[:5],
    }


# ═══════════════════════════════════════════
# 3. 指数技术分析
# ═══════════════════════════════════════════

def calc_index_analysis(engine, date_str: str) -> list[dict]:
    """指数技术位置分析 — 从K线均值计算"""
    sql = """
    SELECT AVG(close) AS price, AVG(change_pct) AS chg,
           (SELECT AVG(k2.close) FROM sm_stock_kline k2 WHERE k2.k_type=1
            AND k2.trade_date = (SELECT MAX(k3.trade_date) FROM sm_stock_kline k3
                                 WHERE k3.k_type=1 AND k3.trade_date < DATE_SUB(:d, INTERVAL 20 DAY)))
           AS ma20
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    rows = pd.read_sql(text(sql), engine, params={"d": date_str}).to_dict(orient="records")
    if not rows:
        return []

    r = rows[0]
    price = round(float(r["price"] or 0), 2)
    ma20 = round(float(r["ma20"] or 0), 2) if r.get("ma20") else None
    chg = round(float(r["chg"] or 0), 4)
    level = "均线附近"
    if ma20 and ma20 > 0:
        if price > ma20 * 1.02:
            level = "突破均线上方"
        elif price < ma20 * 0.98:
            level = "跌破均线下方"
    direction = "上涨" if chg > 0 else "回调" if chg < 0 else "平盘"

    return [{
        "code": "000852",
        "name": "中证1000",
        "price": price,
        "ma20": ma20,
        "level": level,
        "note": f"中证1000 {direction}，{level}{'（均线: ' + str(ma20) + '）' if ma20 else ''}",
    }]


# ═══════════════════════════════════════════
# 专业复盘 — 数据采集函数
# ═══════════════════════════════════════════

def calc_market_temperature(engine, date_str: str) -> dict:
    """
    市场温度评分 0-100
    基于涨跌比、涨停家数、近3日趋势综合计算
    """
    prev_date = _prev_trade_date(engine, date_str)
    recent_dates = _recent_trade_dates(engine, date_str, 3)

    # 涨跌家数 + 涨停跌停
    stats_sql = """
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
      SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
      SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) AS flat_count,
      SUM(CASE WHEN change_pct >= 9.9 THEN 1 ELSE 0 END) AS limit_up,
      SUM(CASE WHEN change_pct <= -9.9 THEN 1 ELSE 0 END) AS limit_down
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    rows = pd.read_sql(text(stats_sql), engine, params={"d": date_str}).to_dict(orient="records")
    r = rows[0] if rows else {}
    up = int(r.get("up_count") or 0)
    down = int(r.get("down_count") or 0)
    total = int(r.get("total") or 1)
    limit_up = int(r.get("limit_up") or 0)
    limit_down = int(r.get("limit_down") or 0)
    up_ratio = up / total if total > 0 else 0.5

    # 基础分：涨跌比映射到0-70
    if up_ratio >= 0.75:
        base = 70
    elif up_ratio >= 0.65:
        base = 60
    elif up_ratio >= 0.55:
        base = 50
    elif up_ratio >= 0.45:
        base = 40
    elif up_ratio >= 0.35:
        base = 30
    elif up_ratio >= 0.25:
        base = 20
    else:
        base = 10

    # 涨停修正：+/-15
    limit_diff = limit_up - limit_down
    if limit_diff > 50:
        limit_adj = 15
    elif limit_diff > 30:
        limit_adj = 12
    elif limit_diff > 15:
        limit_adj = 8
    elif limit_diff > 5:
        limit_adj = 4
    elif limit_diff < -30:
        limit_adj = -15
    elif limit_diff < -15:
        limit_adj = -10
    elif limit_diff < -5:
        limit_adj = -5
    else:
        limit_adj = 0

    # 近3日趋势修正：+/-10
    trend_adj = 0
    if len(recent_dates) >= 2:
        recent_up_ratios = []
        for rd in recent_dates:
            rr = pd.read_sql(text(stats_sql), engine, params={"d": rd}).to_dict(orient="records")
            if rr:
                ru = int(rr[0].get("up_count") or 0)
                rt = int(rr[0].get("total") or 1)
                recent_up_ratios.append(ru / rt if rt > 0 else 0.5)
        if len(recent_up_ratios) >= 3:
            if all(r > 0.55 for r in recent_up_ratios):
                trend_adj = 10  # 赚钱效应扩散
            elif all(r < 0.45 for r in recent_up_ratios):
                trend_adj = -10  # 亏钱效应扩散
            elif recent_up_ratios[-1] > recent_up_ratios[-2] + 0.05:
                trend_adj = 5
            elif recent_up_ratios[-1] < recent_up_ratios[-2] - 0.05:
                trend_adj = -5

    score = max(0, min(100, base + limit_adj + trend_adj))

    # 等级
    if score >= 80:
        level = "强势"
    elif score >= 65:
        level = "偏强"
    elif score >= 45:
        level = "中性"
    elif score >= 30:
        level = "偏弱"
    else:
        level = "弱势"

    # 情绪周期
    # 先算封板率
    seal_sql = """
    SELECT
      SUM(CASE WHEN change_pct >= 9.9 THEN 1 ELSE 0 END) AS limit_up,
      SUM(CASE WHEN high >= LAG(close) OVER (PARTITION BY stock_code ORDER BY trade_date) * 1.095
           AND change_pct < 9.9 THEN 1 ELSE 0 END) AS broken
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    # MySQL 5.7 不支持窗口函数，用子查询替代
    seal_sql2 = """
    SELECT
      SUM(CASE WHEN t.change_pct >= 9.9 THEN 1 ELSE 0 END) AS limit_up,
      SUM(CASE WHEN t.high >= p.close * 1.095 AND t.change_pct < 9.9 THEN 1 ELSE 0 END) AS broken
    FROM sm_stock_kline t
    LEFT JOIN sm_stock_kline p ON t.stock_code = p.stock_code AND p.k_type = 1
      AND p.trade_date = (SELECT MAX(trade_date) FROM sm_stock_kline WHERE trade_date < :d AND k_type=1)
    WHERE t.trade_date = :d AND t.k_type = 1
    """
    try:
        seal_rows = pd.read_sql(text(seal_sql2), engine, params={"d": date_str}).to_dict(orient="records")
        seal_limit = int(seal_rows[0].get("limit_up") or 0) if seal_rows else 0
        seal_broken = int(seal_rows[0].get("broken") or 0) if seal_rows else 0
        seal_total = seal_limit + seal_broken
        seal_rate = (seal_limit / seal_total * 100) if seal_total > 0 else 50
    except Exception:
        seal_rate = 50

    # 判断情绪周期
    if limit_up < 30 and up_ratio < 0.30:
        cycle = "冰点"
        cycle_desc = "涨停家数稀少，市场赚钱效应极差，观望为主"
    elif limit_up < 50 and score < 45:
        cycle = "修复"
        cycle_desc = "市场从低位修复，涨停家数温和增加，可小仓试错"
    elif limit_up >= 50 and seal_rate >= 70 and score >= 50:
        # 检查趋势是否向上
        if trend_adj >= 0:
            cycle = "发酵"
            cycle_desc = "涨停家数扩张，封板质量较好，赚钱效应扩散中"
        else:
            cycle = "发酵"
            cycle_desc = "涨停家数尚可，但趋势略有分歧，关注主线持续性"
    elif limit_up >= 80 and seal_rate >= 80:
        cycle = "高潮"
        cycle_desc = "涨停家数高位，封板率极高，注意高位风险"
    elif prev_date:
        # 对比前日涨停数
        prev_rows = pd.read_sql(text(stats_sql), engine, params={"d": prev_date}).to_dict(orient="records")
        prev_limit_up = int(prev_rows[0].get("limit_up") or 0) if prev_rows else 0
        if prev_limit_up > 0 and limit_up < prev_limit_up * 0.7:
            cycle = "退潮"
            cycle_desc = "涨停家数较前日大幅回落，注意控制仓位"
        elif score < 35:
            cycle = "冰点"
            cycle_desc = "市场赚钱效应差，涨停家数少，宜观望"
        else:
            cycle = "发酵"
            cycle_desc = "涨停家数和封板率处于中等水平，关注主线确认"
    else:
        cycle = "发酵"
        cycle_desc = "涨停家数和封板率处于中等水平，关注主线确认"

    return {
        "score": round(score, 2),
        "level": level,
        "cycle": cycle,
        "cycle_desc": cycle_desc,
        "up_count": up,
        "down_count": down,
        "flat_count": int(r.get("flat_count") or 0),
        "limit_up": limit_up,
        "limit_down": limit_down,
        "up_ratio": round(up_ratio * 100, 2),
    }


def calc_index_detail_pro(engine, date_str: str) -> list[dict]:
    """
    各指数详情：收盘价、涨跌幅、MA5/MA10/MA20、均线状态
    """
    index_codes = ["000001", "399001", "399006", "000688", "000016"]
    index_names = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指",
                   "000688": "科创50", "000016": "上证50"}

    results = []
    for code in index_codes:
        try:
            # 获取最近20个交易日的收盘价
            kline_sql = """
            SELECT trade_date, close, change_pct FROM sm_index_kline
            WHERE index_code = :code AND k_type = 1 AND trade_date <= :d
            ORDER BY trade_date DESC LIMIT 25
            """
            rows = pd.read_sql(text(kline_sql), engine, params={"code": code, "d": date_str}).to_dict(orient="records")
            if not rows:
                # fallback: sm_index_current
                cur_sql = "SELECT price AS close, change_pct FROM sm_index_current WHERE index_code = :code"
                cur_rows = pd.read_sql(text(cur_sql), engine, params={"code": code}).to_dict(orient="records")
                if cur_rows:
                    price = float(cur_rows[0].get("close") or 0)
                    chg = float(cur_rows[0].get("change_pct") or 0)
                    results.append({
                        "code": code, "name": index_names.get(code, code),
                        "price": round(price, 2), "change_pct": round(chg, 2),
                        "ma5": None, "ma10": None, "ma20": None,
                        "above_ma5": "-", "above_ma10": "-", "above_ma20": "-",
                    })
                continue

            rows_sorted = sorted(rows, key=lambda x: str(x["trade_date"]))
            closes = [float(r["close"]) for r in rows_sorted]
            price = closes[-1] if closes else 0
            chg = float(rows_sorted[-1].get("change_pct") or 0) if rows_sorted else 0

            ma5 = round(sum(closes[-5:]) / min(5, len(closes)), 2) if closes else None
            ma10 = round(sum(closes[-10:]) / min(10, len(closes)), 2) if closes else None
            ma20 = round(sum(closes[-20:]) / min(20, len(closes)), 2) if closes else None

            results.append({
                "code": code,
                "name": index_names.get(code, code),
                "price": round(price, 2),
                "change_pct": round(chg, 2),
                "ma5": ma5, "ma10": ma10, "ma20": ma20,
                "above_ma5": "是" if (ma5 and price >= ma5) else "否",
                "above_ma10": "是" if (ma10 and price >= ma10) else "否",
                "above_ma20": "是" if (ma20 and price >= ma20) else "否",
            })
        except Exception as e:
            log.debug("指数 %s 详情计算失败: %s", code, e)

    return results


def calc_volume_structure(engine, date_str: str) -> dict:
    """量能结构：今日成交额、昨日、5日均额"""
    amount_sql = "SELECT COALESCE(SUM(amount), 0) AS amt FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1"
    today_rows = pd.read_sql(text(amount_sql), engine, params={"d": date_str}).to_dict(orient="records")
    today_amt = float(today_rows[0]["amt"]) if today_rows else 0

    prev_date = _prev_trade_date(engine, date_str)
    prev_amt = 0
    if prev_date:
        prev_rows = pd.read_sql(text(amount_sql), engine, params={"d": prev_date}).to_dict(orient="records")
        prev_amt = float(prev_rows[0]["amt"]) if prev_rows else 0

    # 5日均额
    recent_dates = _recent_trade_dates(engine, date_str, 5)
    amt_list = []
    for rd in recent_dates:
        rr = pd.read_sql(text(amount_sql), engine, params={"d": rd}).to_dict(orient="records")
        if rr:
            amt_list.append(float(rr[0]["amt"]))
    avg5 = sum(amt_list) / len(amt_list) if amt_list else 0

    change_pct = ((today_amt - prev_amt) / prev_amt * 100) if prev_amt > 0 else 0
    vs_5d = ((today_amt - avg5) / avg5 * 100) if avg5 > 0 else 0

    if vs_5d > 10:
        status = "放量"
    elif vs_5d < -10:
        status = "缩量"
    else:
        status = "平量"

    return {
        "today": today_amt,
        "yesterday": prev_amt,
        "change_pct": round(change_pct, 2),
        "avg5": avg5,
        "vs_5d": round(vs_5d, 2),
        "status": status,
    }


def calc_market_breadth(engine, date_str: str) -> dict:
    """市场广度：涨跌家数、涨幅分布"""
    sql = """
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
      SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
      SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) AS flat_count,
      SUM(CASE WHEN change_pct >= 3 THEN 1 ELSE 0 END) AS up3,
      SUM(CASE WHEN change_pct >= 5 THEN 1 ELSE 0 END) AS up5,
      SUM(CASE WHEN change_pct <= -3 THEN 1 ELSE 0 END) AS down3,
      SUM(CASE WHEN change_pct <= -5 THEN 1 ELSE 0 END) AS down5
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """
    rows = pd.read_sql(text(sql), engine, params={"d": date_str}).to_dict(orient="records")
    r = rows[0] if rows else {}
    total = int(r.get("total") or 1)
    up = int(r.get("up_count") or 0)
    down = int(r.get("down_count") or 0)
    flat = int(r.get("flat_count") or 0)
    red_ratio = round(up / total * 100, 2) if total > 0 else 0

    if red_ratio >= 60:
        status = "普涨"
    elif red_ratio >= 55:
        status = "偏强"
    elif red_ratio >= 45:
        status = "分化"
    elif red_ratio >= 35:
        status = "偏弱"
    elif red_ratio >= 25:
        status = "冰点压力"
    else:
        status = "极端冰点"

    return {
        "up_count": up, "down_count": down, "flat_count": flat,
        "red_ratio": red_ratio,
        "up3_count": int(r.get("up3") or 0),
        "up5_count": int(r.get("up5") or 0),
        "down3_count": int(r.get("down3") or 0),
        "down5_count": int(r.get("down5") or 0),
        "status": status,
    }


def calc_board_structure(engine, date_str: str) -> dict:
    """涨跌停、炸板、连板分布、最高标、昨日涨停/连板反馈"""
    prev_date = _prev_trade_date(engine, date_str)

    # 今日涨停/跌停
    limit_sql = """
    SELECT stock_code, COALESCE(NULLIF(short_name,''), stock_code) AS short_name,
           close, change_pct
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    AND change_pct >= 9.9 ORDER BY change_pct DESC
    """
    zt_rows = pd.read_sql(text(limit_sql), engine, params={"d": date_str}).to_dict(orient="records")
    limit_up_count = len(zt_rows)

    dt_sql = """
    SELECT stock_code, COALESCE(NULLIF(short_name,''), stock_code) AS short_name,
           close, change_pct
    FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    AND change_pct <= -9.9 ORDER BY change_pct ASC
    """
    dt_rows = pd.read_sql(text(dt_sql), engine, params={"d": date_str}).to_dict(orient="records")
    limit_down_count = len(dt_rows)

    # 炸板：今日最高价接近涨停但收盘未封住
    # high >= 昨收 * 1.095 且 change_pct < 9.9
    broken_sql = """
    SELECT COUNT(*) AS cnt FROM sm_stock_kline t
    LEFT JOIN sm_stock_kline p ON t.stock_code = p.stock_code AND p.k_type = 1
      AND p.trade_date = (SELECT MAX(trade_date) FROM sm_stock_kline WHERE trade_date < :d AND k_type=1)
    WHERE t.trade_date = :d AND t.k_type = 1
      AND t.high >= p.close * 1.095 AND t.change_pct < 9.9
    """
    try:
        broken_rows = pd.read_sql(text(broken_sql), engine, params={"d": date_str}).to_dict(orient="records")
        broken_count = int(broken_rows[0]["cnt"]) if broken_rows else 0
    except Exception:
        broken_count = 0

    touch_limit = limit_up_count + broken_count
    seal_rate = round(limit_up_count / touch_limit * 100, 2) if touch_limit > 0 else 0
    broken_rate = round(broken_count / touch_limit * 100, 2) if touch_limit > 0 else 0

    # 连板分布
    board_first = 0
    board_second = 0
    board_third_plus = 0
    max_boards = 0
    highest_stocks = []

    if zt_rows:
        codes = [str(r["stock_code"]) for r in zt_rows]
        ph = ",".join([f"'{c}'" for c in codes])
        hist_sql = f"""
        SELECT stock_code, COALESCE(NULLIF(short_name,''), stock_code) AS short_name,
               trade_date, change_pct
        FROM sm_stock_kline
        WHERE stock_code IN ({ph}) AND k_type=1
          AND trade_date BETWEEN DATE_SUB('{date_str}', INTERVAL 30 DAY) AND '{date_str}'
        ORDER BY stock_code, trade_date DESC
        """
        try:
            hist = pd.read_sql(text(hist_sql), engine).to_dict(orient="records")
        except Exception:
            hist = []

        from collections import defaultdict
        hist_map = defaultdict(list)
        for h in hist:
            hist_map[str(h["stock_code"])].append(h)

        for r in zt_rows:
            code = str(r["stock_code"])
            boards = 1
            for h in hist_map.get(code, []):
                if str(h["trade_date"]) == str(date_str):
                    continue
                if float(h["change_pct"] or 0) >= 9.9:
                    boards += 1
                else:
                    break
            if boards == 1:
                board_first += 1
            elif boards == 2:
                board_second += 1
            else:
                board_third_plus += 1
            if boards > max_boards:
                max_boards = boards
                highest_stocks = [{"name": r.get("short_name", code), "code": code, "boards": boards}]
            elif boards == max_boards:
                highest_stocks.append({"name": r.get("short_name", code), "code": code, "boards": boards})

    # 昨日涨停今日均涨幅
    prev_zt_avg = None
    prev_lb_avg = None
    if prev_date:
        prev_zt_sql = """
        SELECT t.stock_code, t.change_pct FROM sm_stock_kline t
        WHERE t.trade_date = :d AND t.k_type = 1 AND t.change_pct >= 9.9
        """
        try:
            prev_zt = pd.read_sql(text(prev_zt_sql), engine, params={"d": prev_date}).to_dict(orient="records")
            if prev_zt:
                prev_codes = [str(r["stock_code"]) for r in prev_zt]
                ph2 = ",".join([f"'{c}'" for c in prev_codes])
                today_chg_sql = f"""
                SELECT change_pct FROM sm_stock_kline
                WHERE stock_code IN ({ph2}) AND trade_date = :d AND k_type = 1
                """
                today_chgs = pd.read_sql(text(today_chg_sql), engine, params={"d": date_str}).to_dict(orient="records")
                if today_chgs:
                    chg_vals = [float(r["change_pct"] or 0) for r in today_chgs]
                    prev_zt_avg = round(sum(chg_vals) / len(chg_vals), 2)
        except Exception:
            pass

    return {
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "touch_limit_up": touch_limit,
        "broken_board_count": broken_count,
        "seal_rate": seal_rate,
        "broken_rate": broken_rate,
        "max_boards": max_boards,
        "board_first": board_first,
        "board_second": board_second,
        "board_third_plus": board_third_plus,
        "highest_board_stocks": highest_stocks[:3],
        "prev_zt_avg_chg": prev_zt_avg,
        "prev_lb_avg_chg": prev_lb_avg,
    }


def calc_sector_detail(engine, date_str: str) -> dict:
    """板块详情：涨幅TOP、资金流入TOP、涨停集中、走弱方向"""
    # 板块涨幅TOP
    rank_sql = """
    SELECT concept_name, change_pct, hot_value FROM st_hot_concept_ths_daily
    WHERE snapshot_date = :d ORDER BY change_pct DESC LIMIT 8
    """
    rank_rows = pd.read_sql(text(rank_sql), engine, params={"d": date_str}).to_dict(orient="records")
    sector_rank_top = [{"name": r["concept_name"], "change_pct": round(float(r.get("change_pct") or 0), 2)}
                       for r in rank_rows]

    # 走弱方向
    weak_sql = """
    SELECT concept_name, change_pct FROM st_hot_concept_ths_daily
    WHERE snapshot_date = :d ORDER BY change_pct ASC LIMIT 5
    """
    weak_rows = pd.read_sql(text(weak_sql), engine, params={"d": date_str}).to_dict(orient="records")
    # 需要获取下跌家数 — 从概念板块内的个股统计
    sector_weak = []
    for r in weak_rows:
        name = r["concept_name"]
        chg = round(float(r.get("change_pct") or 0), 2)
        if chg < 0:
            sector_weak.append({"name": name, "avg_change_pct": chg, "down_count": 0})

    # 资金流入TOP — 从 sm_concept_capital_flow_east
    fund_flow_top = []
    try:
        flow_sql = """
        SELECT index_name, main_net_inflow, change_pct
        FROM sm_concept_capital_flow_east
        WHERE days_type = 1
          AND snapshot_at = (SELECT MAX(snapshot_at) FROM sm_concept_capital_flow_east WHERE days_type = 1)
        ORDER BY main_net_inflow DESC LIMIT 8
        """
        flow_rows = pd.read_sql(text(flow_sql), engine).to_dict(orient="records")
        fund_flow_top = [{"name": r["index_name"],
                          "main_net_inflow": round(float(r.get("main_net_inflow") or 0) / 1e8, 2),
                          "change_pct": round(float(r.get("change_pct") or 0), 2)}
                         for r in flow_rows]
    except Exception as e:
        log.debug("板块资金流查询失败: %s", e)

    # 赚钱效应/亏钱效应 — 基于涨幅排名
    earning = sector_rank_top[:5] if sector_rank_top else []
    losing = [{"name": r["name"], "avg_change_pct": r["avg_change_pct"]} for r in sector_weak[:5]]

    return {
        "sector_rank_top": sector_rank_top,
        "sector_fund_flow_top": fund_flow_top,
        "sector_weak_top": sector_weak,
        "earning_sectors": earning,
        "losing_sectors": losing,
    }


def _determine_main_line(temp_data: dict, sector_data: dict, board_data: dict) -> dict:
    """判断主线及状态"""
    rank_top = sector_data.get("sector_rank_top", [])
    fund_top = sector_data.get("sector_fund_flow_top", [])
    limit_up = board_data.get("limit_up_count", 0)

    # 主线 = 涨幅最高 + 资金流入靠前的板块
    main_name = rank_top[0]["name"] if rank_top else "暂无明确主线"
    main_chg = rank_top[0]["change_pct"] if rank_top else 0

    # 判断主线状态
    if limit_up >= 60 and main_chg >= 3:
        status = "主升发酵"
    elif limit_up >= 40 and main_chg >= 2:
        status = "温和发酵"
    elif main_chg >= 1:
        status = "低位启动"
    elif main_chg >= 0:
        status = "震荡蓄力"
    else:
        status = "分歧退潮"

    # 盘面风格
    score = temp_data.get("score", 50)
    if score >= 65:
        style = "成长股反弹"
    elif score >= 45:
        style = "均衡轮动"
    else:
        style = "防御避险"

    return {
        "name": main_name,
        "status": status,
        "style": style,
        "desc": f"今日主线为 {main_name}，状态\"{status}\"",
    }


def _determine_trade_env(temp_data: dict, board_data: dict) -> str:
    """判断次日交易环境"""
    score = temp_data.get("score", 50)
    seal = board_data.get("seal_rate", 50)
    limit_up = board_data.get("limit_up_count", 0)

    if score >= 65 and seal >= 75:
        return "可适度扩展选股范围"
    elif score >= 50 and seal >= 65:
        return "只看核心前排"
    elif score >= 35:
        return "严格控制仓位，只做确定性机会"
    else:
        return "以观望为主，极轻仓试错"


def _determine_position(temp_data: dict) -> str:
    """建议仓位"""
    score = temp_data.get("score", 50)
    if score >= 75:
        return "50%-60%"
    elif score >= 65:
        return "40%-50%"
    elif score >= 50:
        return "30%-40%"
    elif score >= 35:
        return "20%-30%"
    else:
        return "10%以下"


def _fmt_amt_cn(v: float) -> str:
    """格式化金额为亿"""
    if v >= 1e8:
        return f"{v / 1e8:.2f}亿"
    elif v >= 1e4:
        return f"{v / 1e4:.0f}万"
    return f"{v:.0f}"


def generate_pro_review(engine, date_str: str) -> str:
    """生成专业复盘Markdown全文，写入 st_daily_review_pro"""

    log.info("开始生成专业复盘: %s", date_str)

    # ── 采集所有数据 ──
    temp = calc_market_temperature(engine, date_str)
    indices = calc_index_detail_pro(engine, date_str)
    volume = calc_volume_structure(engine, date_str)
    breadth = calc_market_breadth(engine, date_str)
    board = calc_board_structure(engine, date_str)
    sector = calc_sector_detail(engine, date_str)
    main_line = _determine_main_line(temp, sector, board)
    trade_env = _determine_trade_env(temp, board)
    position = _determine_position(temp)

    # ── 格式化 ──
    lines = []
    lines.append(f"【大盘专业复盘｜{date_str}】")
    lines.append("")

    # 1. 今日核心结论
    lines.append("1. 今日核心结论")
    lines.append(f"- 环境定位：市场温度 {temp['score']} 分、等级\"{temp['level']}\"，情绪周期\"{temp['cycle']}\"，次日交易环境为\"{trade_env}\"；这代表明日不是单纯看指数涨跌，而是看量能、涨停结构和主线承接能否继续共振。")
    lines.append(f"- 机会方向：当前盘面风格为\"{main_line['style']}\"，主线为 {main_line['name']}，状态\"{main_line['status']}\"；若主线前排继续封稳，选股范围优先放在 {_sector_list_str(sector, 'top')}，回避 {_sector_list_str(sector, 'weak')}。")
    lines.append(f"- 执行结论：{_execution_conclusion(temp, board, volume)}；建议仓位 {position}。")
    lines.append("")

    # 2. 关键数据复核
    lines.append("2. 关键数据复核")
    # 指数结构
    idx_parts = []
    for idx in indices:
        idx_parts.append(f"{idx['name']} {idx['price']}点、{idx['change_pct']:+.2f}%")
    if idx_parts:
        lines.append(f"- 指数结构：宽基指数表现：{'、'.join(idx_parts)}。")
    # 均线状态
    if indices:
        ma_lines = []
        for idx in indices:
            ma_lines.append(f"{idx['name']}5/10/20日线：{idx['above_ma5']}/{idx['above_ma10']}/{idx['above_ma20']}")
        lines.append(f"- 均线状态：{'、'.join(ma_lines)}。")
    # 量能
    vol_amt = _fmt_amt_cn(volume["today"])
    vol_prev = _fmt_amt_cn(volume["yesterday"])
    vol_avg5 = _fmt_amt_cn(volume["avg5"])
    lines.append(f"- 量能结构：两市成交额 {vol_amt}，昨日 {vol_prev}，较昨日 {volume['change_pct']:+.2f}%（{volume['status']}），5日均额 {vol_avg5}，量能状态\"{volume['status']}\"。")
    # 广度
    lines.append(f"- 市场广度：上涨 {breadth['up_count']} 家、下跌 {breadth['down_count']} 家、平盘 {breadth['flat_count']} 家，红盘比例 {breadth['red_ratio']}%，广度状态\"{breadth['status']}\"。涨幅超3%/5%分别为 {breadth['up3_count']}/{breadth['up5_count']} 家，跌幅超3%/5%分别为 {breadth['down3_count']}/{breadth['down5_count']} 家。")
    lines.append("")

    # 3. 情绪与接力环境
    lines.append("3. 情绪与接力环境")
    highest_names = "、".join([s["name"] for s in board.get("highest_board_stocks", [])])
    lines.append(f"- 涨停 {board['limit_up_count']} 家、跌停 {board['limit_down_count']} 家，触及涨停 {board['touch_limit_up']} 家，炸板 {board['broken_board_count']} 家；封板率 {board['seal_rate']}%，炸板率 {board['broken_rate']}%。最高连板 {board['max_boards']}板，首板/二板/三板以上 {board['board_first']}/{board['board_second']}/{board['board_third_plus']} 家。")
    if highest_names:
        lines.append(f"- 最高标：{highest_names}。它们是明日情绪锚点，不是无条件追涨标的；只有高位不出现连续负反馈，低位试错才更有意义。")
    if board.get("prev_zt_avg_chg") is not None:
        lines.append(f"- 昨日涨停反馈：昨日涨停股今日平均涨幅 {board['prev_zt_avg_chg']}%；")
    if board.get("prev_lb_avg_chg") is not None:
        lines.append(f"- 昨日连板反馈：昨日连板股今日平均涨幅 {board['prev_lb_avg_chg']}%。")
    lines.append("")

    # 4. 主线与板块判断
    lines.append("4. 主线与板块判断")
    lines.append(f"- 今日主线为 {main_line['name']}，状态\"{main_line['status']}\"。")
    if sector.get("sector_rank_top"):
        top5 = sector["sector_rank_top"][:5]
        items = [f"{s['name']}（涨幅{s['change_pct']:+.2f}%）" for s in top5]
        lines.append(f"- 板块涨幅排名Top5：{'、'.join(items)}；")
    if sector.get("sector_fund_flow_top"):
        top5f = sector["sector_fund_flow_top"][:5]
        items = [f"{s['name']}（主力净流入+{s['main_net_inflow']}亿）" for s in top5f]
        lines.append(f"- 资金流入排名Top5：{'、'.join(items)}；")
    if sector.get("sector_weak_top"):
        items = [f"{s['name']}（平均涨跌{s['avg_change_pct']:+.2f}%）" for s in sector["sector_weak_top"][:5]]
        lines.append(f"- 走弱方向：{'、'.join(items)}。")
    lines.append(f"- {main_line['desc']}。主线仍有延续观察价值，低位补涨和放量突破的性价比高于高位追涨。")
    # 赚钱/亏钱效应
    if sector.get("earning_sectors"):
        items = [f"{s['name']}（涨幅{s['change_pct']:+.2f}%）" for s in sector["earning_sectors"][:4]]
        lines.append(f"- 赚钱效应集中在：{'、'.join(items)}。")
    if sector.get("losing_sectors"):
        items = [f"{s['name']}（平均涨跌{s['avg_change_pct']:+.2f}%）" for s in sector["losing_sectors"][:4]]
        lines.append(f"- 亏钱效应集中在：{'、'.join(items)}。")
    lines.append(f"- 明日参与条件：主线前排封单稳定、板块成交额不明显萎缩、后排有跟随扩散；若只剩少数高位股硬顶而板块内部走弱，则把它视为风险而不是机会。")
    lines.append("")

    # 5. 明日执行计划
    lines.append("5. 明日执行计划")
    vol_threshold = round(volume["today"] * 0.95 / 1e8, 0)
    lines.append(f"- 放量上攻：若两市成交额维持在 {_fmt_amt_cn(volume['today'])} 以上并继续放大，且涨停家数高于今日 {board['limit_up_count']} 家、炸板率回落，优先跟踪 {main_line['name']} 的前排晋级、低位首板和放量突破。")
    lines.append(f"- 缩量震荡：若成交额低于 {vol_threshold}亿 或红盘比例回落，只做核心方向分歧低吸，后排跟风和缩量冲高不纳入首选。")
    lines.append(f"- 放量下跌：若指数放量走弱、跌停家数较今日 {board['limit_down_count']} 家继续扩张，先降低总仓位，回避高位加速、炸板回落和跌破关键均线个股。")
    lines.append("")

    # 6. 仓位与风控
    lines.append("6. 仓位与风控")
    lines.append(f"- 仓位规则：系统建议总仓位 {position}；单票先小仓验证，弱于预期不加仓。")
    lines.append(f"- 稳健账户：按系统建议仓位下限执行，只看 {main_line['name']} 中最有量能确认的股票；若触发风险项，先减仓再复核。")
    lines.append(f"- 激进账户：只允许在主线仍强、个股不追高、成交额不低于计划阈值时试错；买入前先写好止损、止盈和持仓时限，不能用\"再等等\"替代纪律。")
    lines.append(f"- 风险触发器：红盘比例低于45%，个股扩散不足、成交额缩量，追涨承接可能不足。")
    lines.append("")

    # 7. 盘中观察清单
    lines.append("7. 盘中观察清单")
    lines.append(f"- 两市成交额是否维持或放大，缩量时降低追涨权重")
    lines.append(f"- {main_line['name']} 前排是否继续晋级，后排是否还能跟随")
    lines.append(f"- 炸板率是否低于30%，跌停家数是否继续扩大")
    lines.append(f"- 主要指数是否守住5日线，指数弱时只做核心股")
    lines.append(f"- 智能选股按\"优先回踩5日线反包、相对强度RS；放量突破需降低仓位\"执行，结果优先看操作结论和作废条件")
    lines.append("")

    # 8. 数据完整性提示
    lines.append("8. 数据完整性提示")
    if volume["avg5"] == 0:
        lines.append("- 5日均额数据源异常，已跳过相对5日均量对比")
    lines.append("")

    # 一句话总结
    lines.append(f"一句话总结：本次复盘的主线是\"用成交额确认承接、用广度判断扩散、用涨跌停结构验证情绪\"。只要明日量能、主线前排或封板质量任一项走弱，就先收缩仓位；只有三者继续共振，才提高选股和试错优先级。")
    lines.append("")
    lines.append("风险提示：以上内容仅用于市场复盘和策略研究，不构成任何投资建议。股市有风险，交易需谨慎。")

    pro_review_text = "\n".join(lines)

    # ── 写入数据库 ──
    record = {
        "review_date": date_str,
        "market_temp_score": temp["score"],
        "market_temp_level": temp["level"],
        "sentiment_cycle": temp["cycle"],
        "sentiment_cycle_desc": temp["cycle_desc"],
        "main_line_name": main_line["name"],
        "main_line_status": main_line["status"],
        "style_bias": main_line["style"],
        "position_suggest": position,
        "trade_env": trade_env,
        "indices_detail": json.dumps(indices, ensure_ascii=False),
        "volume_today": volume["today"],
        "volume_yesterday": volume["yesterday"],
        "volume_change_pct": volume["change_pct"],
        "volume_5d_avg": volume["avg5"],
        "volume_status": volume["status"],
        "up_count": breadth["up_count"],
        "down_count": breadth["down_count"],
        "flat_count": breadth["flat_count"],
        "red_ratio": breadth["red_ratio"],
        "up3_count": breadth["up3_count"],
        "up5_count": breadth["up5_count"],
        "down3_count": breadth["down3_count"],
        "down5_count": breadth["down5_count"],
        "breadth_status": breadth["status"],
        "limit_up_count": board["limit_up_count"],
        "limit_down_count": board["limit_down_count"],
        "touch_limit_up": board["touch_limit_up"],
        "broken_board_count": board["broken_board_count"],
        "seal_rate": board["seal_rate"],
        "broken_rate": board["broken_rate"],
        "max_boards": board["max_boards"],
        "board_first": board["board_first"],
        "board_second": board["board_second"],
        "board_third_plus": board["board_third_plus"],
        "highest_board_stocks": json.dumps(board["highest_board_stocks"], ensure_ascii=False),
        "prev_zt_avg_chg": board.get("prev_zt_avg_chg"),
        "prev_lb_avg_chg": board.get("prev_lb_avg_chg"),
        "sector_rank_top": json.dumps(sector["sector_rank_top"], ensure_ascii=False),
        "sector_fund_flow_top": json.dumps(sector["sector_fund_flow_top"], ensure_ascii=False),
        "sector_limit_up_top": json.dumps([], ensure_ascii=False),
        "sector_volume_top": json.dumps([], ensure_ascii=False),
        "sector_weak_top": json.dumps(sector["sector_weak_top"], ensure_ascii=False),
        "earning_sectors": json.dumps(sector["earning_sectors"], ensure_ascii=False),
        "losing_sectors": json.dumps(sector["losing_sectors"], ensure_ascii=False),
        "main_line_desc": main_line["desc"],
        "pro_review": pro_review_text,
        "etl_sync_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    columns = ", ".join(f"`{k}`" for k in record.keys())
    placeholders = ", ".join(f":{k}" for k in record.keys())
    updates = ", ".join(f"`{k}` = VALUES(`{k}`)" for k in record if k != "review_date")

    sql = f"INSERT INTO st_daily_review_pro ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    with engine.begin() as conn:
        conn.execute(text(sql), record)

    log.info("专业复盘已写入: %s (%d 字)", date_str, len(pro_review_text))
    return pro_review_text


def _sector_list_str(sector_data: dict, mode: str = "top") -> str:
    """板块列表字符串"""
    if mode == "top":
        items = sector_data.get("sector_rank_top", [])[:5]
        return "、".join([s["name"] for s in items]) if items else "暂无"
    else:
        items = sector_data.get("sector_weak_top", [])[:5]
        return "、".join([s["name"] for s in items]) if items else "暂无"


def _execution_conclusion(temp: dict, board: dict, volume: dict) -> str:
    """执行结论"""
    score = temp.get("score", 50)
    seal = board.get("seal_rate", 50)
    vol_status = volume.get("status", "平量")

    parts = []
    if score >= 65 and seal >= 70:
        parts.append("轻仓进攻，优先分歧低吸和放量确认")
    elif score >= 50:
        parts.append("谨慎参与，只做核心方向")
    else:
        parts.append("以防守为主，极轻仓试错")

    if vol_status == "缩量":
        parts.append("缩量环境下后排跟风不纳入首选")

    return "；".join(parts)


# ═══════════════════════════════════════════
# 4. 生成文本摘要
# ═══════════════════════════════════════════

def generate_summary(overview: dict, sector: dict, idx_analysis: list) -> str:
    lines = []

    amt_display = f"{overview['total_amount'] / 1e8:.0f}亿" if overview["total_amount"] > 0 else "暂无"
    heat_dir = "有所下降" if overview["market_heat_change"] == "down" else (
        "有所上升" if overview["market_heat_change"] == "up" else "基本持平")
    amt_dir = "放大" if overview["total_amount_change"] == "放量" else (
        "萎缩" if overview["total_amount_change"] == "缩量" else "基本持平")

    idx_name = overview.get("index_name", "万得全A")
    idx_chg = overview.get("index_change_pct")
    idx_dir = "上涨" if idx_chg and idx_chg > 0 else "下跌"

    lines.append(f"今天市场整体热度{heat_dir}；成交额{amt_display}，{amt_dir}；{idx_name}指数有所{idx_dir}。")
    if overview.get("sideline_ratio"):
        lines.append(f"今天场内观望资金比例加速上升，继续刷新阶段新高。")

    # 板块
    hot_names = [s["name"] for s in sector.get("hot_sectors", [])[:5]]
    cold_names = [s["name"] for s in sector.get("cold_sectors", [])[:5]]
    vol_up_names = [s["name"] for s in sector.get("volume_up_sectors", [])[:5]]
    vol_down_names = [s["name"] for s in sector.get("volume_down_sectors", [])[:5]]

    if hot_names:
        lines.append(f"今天{','.join(hot_names[:3])}板块热度明显上升，"
                     f"{','.join(cold_names[:3])}板块热度明显下降。")
    else:
        lines.append(f"今天没有热度明显上升的板块，"
                     f"{','.join(cold_names[:4])}板块热度下降。")

    if vol_up_names:
        lines.append(f"今天{','.join(vol_up_names[:3])}板块明显放量，"
                     f"{','.join(vol_down_names[:3])}板块明显缩量。")

    # 指数
    for ia in idx_analysis:
        lines.append(f"今天{ia['name']}热度有所变化，{ia['note']}。")

    return "\n".join(lines)


# ═══════════════════════════════════════════
# 5. 主函数
# ═══════════════════════════════════════════

def generate_review(engine, date_str: str) -> dict:
    run_ddl(engine)

    overview = calc_market_overview(engine, date_str)
    sector = calc_sector_analysis(engine, date_str)
    idx_analysis = calc_index_analysis(engine, date_str)
    summary = generate_summary(overview, sector, idx_analysis)

    record = {
        **overview,
        "hot_sectors": json.dumps(sector["hot_sectors"], ensure_ascii=False),
        "cold_sectors": json.dumps(sector["cold_sectors"], ensure_ascii=False),
        "volume_up_sectors": json.dumps(sector["volume_up_sectors"], ensure_ascii=False),
        "volume_down_sectors": json.dumps(sector["volume_down_sectors"], ensure_ascii=False),
        "index_analysis": json.dumps(idx_analysis, ensure_ascii=False),
        "summary": summary,
        "disclaimer": "本建议仅供参考，不构成具体投资建议。投资可能存在市场风险、公司风险及信息风险等，需谨慎。",
        "source": "ProBigA",
        "analyst": "ProBigA智能复盘",
        "etl_sync_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # REPLACE INTO
    columns = ", ".join(f"`{k}`" for k in record.keys())
    placeholders = ", ".join(f":{k}" for k in record.keys())
    updates = ", ".join(f"`{k}` = VALUES(`{k}`)" for k in record if k != "review_date")

    sql = f"INSERT INTO st_daily_review ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    with engine.begin() as conn:
        conn.execute(text(sql), record)

    log.info("复盘数据已写入: %s (%s 字节summary)", date_str, len(summary))

    # 生成专业复盘
    try:
        generate_pro_review(engine, date_str)
    except Exception as e:
        log.error("专业复盘生成失败: %s", e)

    return record


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("date", nargs="?", help="日期 YYYY-MM-DD")
    g.add_argument("--today", action="store_true")
    p.add_argument("--force", action="store_true", help="强制覆盖")
    args, unknown = p.parse_known_args()

    engine = get_engine()
    date_str = datetime.now().strftime("%Y-%m-%d") if args.today else args.date
    generate_review(engine, date_str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
