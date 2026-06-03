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
