#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量筛选「建议买」股票 — 量化初选 + AI 评分二次过滤

流程：
  1. 用多种量化策略（趋势强势、低位启动、资金流入）初选候选股
  2. 去重合并
  3. 对每只候选股调用 DeepSeek AI 评分（综合评分 0-100）
  4. 只保留评分 >= 70 的股票，输出最终推荐列表

用法：
  python tools/find_buy_candidates.py
  python tools/find_buy_candidates.py --date 2025-05-30 --min-score 65 --top 30
  python tools/find_buy_candidates.py --skip-ai   # 只跑量化筛选，不调 AI

环境变量：
  MYSQL_URL      — 数据库连接（必须显式配置；也可使用 DATABASE_URL）
  DEEPSEEK_API_KEY — DeepSeek API Key（不设则跳过 AI 评分）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, resolve_tool_mysql_url


def _engine():
    return create_tool_engine()


def _latest_trade_date(engine, table: str, col: str = "trade_date") -> str:
    q = text(f"SELECT MAX({col}) FROM {table}")
    with engine.connect() as conn:
        r = conn.execute(q).scalar()
    return str(r) if r else date.today().isoformat()


# ═══════════════════════════════════════════
# 量化筛选函数（复用 screen_stocks.py 逻辑）
# ═══════════════════════════════════════════

def screen_trend_strong(engine, trade_date: str, top: int = 50) -> pd.DataFrame:
    """强势趋势票：四线多头 + 连续站上MA5 + 创新高 + 温和量比"""
    from tools.screen_stocks import run_trend_strong
    return run_trend_strong(
        engine, trade_date, top,
        k_type=1, adjust_type=1,
        trend_days=10, ma_slope_min=0.5,
        vol_ratio_min=0.8, vol_ratio_max=2.5,
        max_60d_gain=150.0, new_high_pct=0.95,
    )


def screen_low_start(engine, trade_date: str, top: int = 50) -> pd.DataFrame:
    """低位启动：距低点不远 + 放量 + 温和上涨"""
    from tools.screen_stocks import run_low_start
    return run_low_start(
        engine, trade_date, top,
        k_type=1, adjust_type=1,
        low_lookback=60, max_from_low=0.28,
        vol_boost=1.25, min_chg=2.0, max_chg=10.5,
    )


def screen_flow(engine, trade_date: str, top: int = 50) -> pd.DataFrame:
    """资金流入：主力净流入排名"""
    from tools.screen_stocks import run_flow
    return run_flow(engine, trade_date, top, min_main=5_000_000)


def screen_trend(engine, trade_date: str, top: int = 50) -> pd.DataFrame:
    """趋势多头：MA5>MA10>MA20 且收盘在MA5上方"""
    from tools.screen_stocks import run_trend
    return run_trend(engine, trade_date, top, k_type=1, adjust_type=1, min_chg=0)


def collect_candidates(engine, trade_date: str, top_per_mode: int = 30) -> pd.DataFrame:
    """运行多种策略，去重合并候选股"""
    all_dfs = []
    modes = [
        ("trend_strong", screen_trend_strong),
        ("low_start", screen_low_start),
        ("trend", screen_trend),
        ("flow", screen_flow),
    ]
    for name, fn in modes:
        try:
            df = fn(engine, trade_date, top_per_mode)
            if df is not None and not df.empty:
                df["_source"] = name
                all_dfs.append(df)
                print(f"  [{name}] 筛出 {len(df)} 只")
        except Exception as e:
            print(f"  [{name}] 失败: {e}")

    if not all_dfs:
        return pd.DataFrame()

    # 合并去重，保留 stock_code + short_name
    combined = pd.concat(all_dfs, ignore_index=True)
    combined["stock_code"] = combined["stock_code"].astype(str).str.strip().str.zfill(6)

    # 去掉 ST 股
    if "short_name" in combined.columns:
        combined = combined[~combined["short_name"].fillna("").str.contains("ST", case=False)]

    # 去掉非主板（只保留 0/6 开头）
    combined = combined[combined["stock_code"].str.match(r"^(0|6)")]

    # 去重，记录来源
    dedup = combined.drop_duplicates(subset=["stock_code"])
    sources = combined.groupby("stock_code")["_source"].apply(lambda x: "+".join(sorted(set(x)))).reset_index()
    sources.columns = ["stock_code", "sources"]
    dedup = dedup.merge(sources, on="stock_code", how="left")

    return dedup


# ═══════════════════════════════════════════
# AI 评分
# ═══════════════════════════════════════════

def fetch_stock_summary(engine, code: str) -> dict:
    """从数据库拉取单只股票的多维度数据摘要"""
    # K线
    klines = pd.read_sql(text("""
        SELECT trade_date, close, change_pct, volume, turnover_ratio
        FROM sm_stock_kline WHERE stock_code = :c AND k_type=1
        ORDER BY trade_date DESC LIMIT 20
    """), engine, params={"c": code})

    # 资金流
    flow = pd.read_sql(text("""
        SELECT trade_date, main_net_inflow, lg_net_inflow
        FROM sm_stock_capital_flow_daily WHERE stock_code = :c
        ORDER BY trade_date DESC LIMIT 5
    """), engine, params={"c": code})

    # 基本信息
    info = pd.read_sql(text("""
        SELECT stock_code, short_name FROM si_all_code WHERE stock_code = :c LIMIT 1
    """), engine, params={"c": code})

    name = info.iloc[0]["short_name"] if len(info) > 0 else code

    # 构建摘要
    parts = [f"股票：{name}({code})"]

    if len(klines) > 0:
        latest = klines.iloc[0]
        parts.append(f"最新收盘：{latest['close']}，涨跌幅：{latest['change_pct']}%，换手率：{latest.get('turnover_ratio', '-')}%")
        if len(klines) >= 5:
            chg5 = ", ".join([f"{r['trade_date']}: {r['change_pct']}%" for _, r in klines.head(5).iterrows()])
            parts.append(f"近5日涨跌：{chg5}")
        # 均线
        closes = klines["close"].astype(float).tolist()
        if len(closes) >= 5:
            parts.append(f"MA5: {sum(closes[:5])/5:.2f}")
        if len(closes) >= 10:
            parts.append(f"MA10: {sum(closes[:10])/10:.2f}")
        if len(closes) >= 20:
            parts.append(f"MA20: {sum(closes[:20])/20:.2f}")

    if len(flow) > 0:
        main_sum = flow["main_net_inflow"].astype(float).sum()
        parts.append(f"近{len(flow)}日主力净流入合计：{main_sum/10000:.0f}万")

    return {"code": code, "name": name, "summary": "；".join(parts)}


def ai_score_stock(api_key: str, summary: str) -> dict:
    """调用 DeepSeek 对单只股票评分"""
    import httpx

    prompt = f"""你是一个全球顶尖的A股交易员。根据以下数据，给出评分和简要结论。

{summary}

请返回以下JSON格式（不要返回其他内容）：
{{
  "score": 综合评分(0-100),
  "scores": {{
    "fundamental": 基本面评分(0-100),
    "capital": 资金面评分(0-100),
    "valuation": 估值评分(0-100),
    "technical": 技术面评分(0-100)
  }},
  "reason": "一句话说明为什么给这个分数"
}}"""

    try:
        resp = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3},
            timeout=30,
        )
        content = resp.json()["choices"][0]["message"]["content"]
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]
        return json.loads(content.strip())
    except Exception as e:
        return {"score": None, "reason": f"AI评分失败: {e}"}


# ═══════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="批量筛选建议买的股票（量化+AI）")
    p.add_argument("--date", type=str, default="", help="交易日 YYYY-MM-DD（默认自动取最新）")
    p.add_argument("--top", type=int, default=20, help="最终输出最多几只（默认20）")
    p.add_argument("--min-score", type=int, default=70, help="AI综合评分下限（默认70）")
    p.add_argument("--per-mode", type=int, default=30, help="每种策略初选数量（默认30）")
    p.add_argument("--skip-ai", action="store_true", help="跳过AI评分，只输出量化筛选结果")
    p.add_argument("--csv", type=str, default="", help="输出CSV路径")
    args = p.parse_args()

    eng = _engine()
    trade_date = args.date.strip() or _latest_trade_date(eng, "sm_stock_kline")
    print(f"═══ 交易日: {trade_date} ═══\n")

    # 第一步：量化筛选
    print("【第一步】量化策略筛选...")
    candidates = collect_candidates(eng, trade_date, args.per_mode)
    if candidates.empty:
        print("无候选股，检查日期是否有数据。")
        return

    print(f"\n共 {len(candidates)} 只候选股（去重后）\n")

    if args.skip_ai:
        cols = [c for c in ["stock_code", "short_name", "change_pct", "close", "sources"] if c in candidates.columns]
        result = candidates[cols].head(args.top)
        print(result.to_string(index=False))
        if args.csv:
            result.to_csv(args.csv, index=False, encoding="utf-8-sig")
            print(f"\n已写入: {args.csv}")
        return

    # 第二步：AI 评分
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        print("未设置 DEEPSEEK_API_KEY 环境变量，跳过 AI 评分。")
        print("设置后重新运行，或加 --skip-ai 只看量化结果。")
        cols = [c for c in ["stock_code", "short_name", "change_pct", "close", "sources"] if c in candidates.columns]
        print(candidates[cols].head(args.top).to_string(index=False))
        return

    print(f"【第二步】AI 评分（{len(candidates)} 只，评分下限 {args.min_score}）...")
    results = []
    for i, (_, row) in enumerate(candidates.iterrows()):
        code = str(row["stock_code"]).zfill(6)
        name = row.get("short_name", code)
        print(f"  [{i+1}/{len(candidates)}] {name}({code})...", end=" ", flush=True)

        summary_data = fetch_stock_summary(eng, code)
        ai_result = ai_score_stock(api_key, summary_data["summary"])
        score = ai_result.get("score")
        reason = ai_result.get("reason", "")
        scores = ai_result.get("scores", {})

        print(f"评分: {score} | {reason}")

        if score is not None and score >= args.min_score:
            results.append({
                "stock_code": code,
                "short_name": name,
                "score": score,
                "fundamental": scores.get("fundamental", "-"),
                "capital": scores.get("capital", "-"),
                "valuation": scores.get("valuation", "-"),
                "technical": scores.get("technical", "-"),
                "reason": reason,
                "sources": row.get("sources", ""),
            })

        # 限速，避免 API 过载
        if i < len(candidates) - 1:
            time.sleep(0.5)

    # 输出结果
    print(f"\n{'═'*60}")
    print(f"【最终结果】评分 >= {args.min_score} 的股票：{len(results)} 只")
    print(f"{'═'*60}\n")

    if not results:
        print("没有股票达到评分标准。可以尝试降低 --min-score。")
        return

    result_df = pd.DataFrame(results).sort_values("score", ascending=False).head(args.top)
    print(result_df.to_string(index=False))

    if args.csv:
        result_df.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"\n已写入: {args.csv}")

    return result_df


if __name__ == "__main__":
    main()
