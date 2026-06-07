#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股每日晚报 — 盘后全市场复盘，推送到企微早报机器人

执行:
  python -m biz.evening_review.generate 2026-05-19  # 指定日期
  python -m biz.evening_review.generate               # 默认最新交易日
  python -m biz.evening_review.generate --test        # 不推送，仅打印
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "adata") not in sys.path:
    sys.path.insert(0, str(ROOT / "adata"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("evening")

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
WX_BRIEFING_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b1110965-119d-438b-856d-0d87c751cf13"
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"


def get_engine():
    return create_engine(os.environ.get("MYSQL_URL") or DEFAULT_MYSQL_URL, pool_pre_ping=True)


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_amt(v: float | None) -> str:
    if v is None or v == 0:
        return "-"
    if abs(v) >= 1e8:
        return f"{v/1e8:.1f}亿"
    if abs(v) >= 1e4:
        return f"{v/1e4:.0f}万"
    return f"{v:.0f}"


def _color_tag(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="info">' if v > 0 else '<font color="warning">' if v < 0 else ""


def _color_close(v: float | None) -> str:
    if v is None:
        return ""
    return "</font>" if v != 0 else ""


def _red(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="warning">' if v > 0 else ""  # A股红涨绿跌


def _green(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="info">' if v < 0 else ""


def collect_market_data(engine, date_str: str) -> dict:
    """采集当天盘面数据"""
    data = {}

    # 1. 指数表现
    rows = _query(engine, """
        SELECT i.index_code, i.price, i.change_pct
        FROM sm_index_current i
        WHERE i.index_code IN ('000001','399001','399006','000688','000300')
        ORDER BY FIELD(i.index_code,'000001','399001','399006','000688','000300')
    """)
    nm = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50", "000300": "沪深300"}
    data["指数"] = {nm.get(r["index_code"], r["index_code"]): r for r in rows if r.get("price")}

    # 2. 涨跌家数 & 成交额
    rows = _query(engine, """
        SELECT
            COUNT(*) AS total_cnt,
            SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
            SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt,
            SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) AS flat_cnt,
            COALESCE(SUM(amount), 0) AS total_amt,
            AVG(change_pct) AS avg_chg,
            AVG(turnover_ratio) AS avg_turnover
        FROM sm_stock_kline WHERE trade_date = :d AND k_type = 1
    """, {"d": date_str})
    if rows and rows[0]["total_cnt"]:
        r = rows[0]
        total = max(int(r["total_cnt"] or 0), 1)
        data["涨跌"] = {
            "上涨": int(r["up_cnt"] or 0),
            "下跌": int(r["down_cnt"] or 0),
            "平盘": int(r["flat_cnt"] or 0),
            "上涨比例": round(int(r["up_cnt"] or 0) / total * 100, 1),
        }
        data["成交额"] = float(r["total_amt"] or 0)
        data["均价涨幅"] = float(r["avg_chg"] or 0)
        data["均换手率"] = float(r["avg_turnover"] or 0)

    # 3. 资金流向TOP
    rows = _query(engine, """
        SELECT f.stock_code, COALESCE(s.short_name, f.stock_code) AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code s ON f.stock_code = s.stock_code
        WHERE f.trade_date = :d
        ORDER BY f.main_net_inflow DESC LIMIT 6
    """, {"d": date_str})
    data["净流入TOP"] = [(r["sn"], float(r["main_net_inflow"] or 0)) for r in rows]

    rows = _query(engine, """
        SELECT f.stock_code, COALESCE(s.short_name, f.stock_code) AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code s ON f.stock_code = s.stock_code
        WHERE f.trade_date = :d
        ORDER BY f.main_net_inflow ASC LIMIT 6
    """, {"d": date_str})
    data["净流出TOP"] = [(r["sn"], float(r["main_net_inflow"] or 0)) for r in rows]

    # 4. 热门板块
    rows = _query(engine, """
        SELECT concept_name, change_pct, hot_value, plate_type
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_concept_ths_daily WHERE snapshot_date >= :d)
        ORDER BY plate_type, rank
    """, {"d": date_str})
    data["热门行业"] = [r for r in rows if r.get("plate_type") == 2][:8]
    data["热门概念"] = [r for r in rows if r.get("plate_type") == 1][:10]

    # 5. 融合榜热门个股TOP10
    rows = _query(engine, """
        SELECT f.short_name, f.stock_code, f.change_pct, f.fused_rank,
               t.pop_tag, t.concept_tag
        FROM st_hot_rank_fused f
        LEFT JOIN st_hot_rank_ths t ON t.stock_code = f.stock_code COLLATE utf8mb4_unicode_ci
            AND t.snapshot_date = f.snapshot_date
        WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_fused WHERE snapshot_date >= :d)
        ORDER BY f.fused_rank LIMIT 10
    """, {"d": date_str})
    data["热门个股"] = rows

    # 6. 龙虎榜概要
    rows = _query(engine, """
        SELECT stock_code, short_name, a_net_amount, change_cpt AS change_pct, reason
        FROM st_a_list_daily
        WHERE trade_date = :d
        ORDER BY ABS(a_net_amount) DESC LIMIT 8
    """, {"d": date_str})
    data["龙虎榜"] = rows

    # 7. 涨跌停
    rows = _query(engine, """
        SELECT stock_code, short_name, close, change_pct
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND change_pct >= 0.09
        ORDER BY change_pct DESC LIMIT 8
    """, {"d": date_str})
    data["涨停板"] = rows

    rows = _query(engine, """
        SELECT stock_code, short_name, close, change_pct
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND change_pct <= -0.09
        ORDER BY change_pct ASC LIMIT 5
    """, {"d": date_str})
    data["跌停板"] = rows

    return data


def build_report(data: dict, date_str: str, ai_analysis: str = "") -> str:
    """组装晚报markdown"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")
    lines = [f"## 📊 A股复盘晚报 | {date_display}"]

    # ── 指数 ──
    idx = data.get("指数", {})
    if idx:
        parts = []
        for name, r in idx.items():
            chg = r.get("change_pct")
            color = "warning" if (chg or 0) > 0 else "info" if (chg or 0) < 0 else "comment"
            parts.append(f'<font color="{color}">{name} {_fmt_pct(chg)}</font>')
        lines.append(f"\n**📐 主要指数**  {' · '.join(parts)}")

    # ── 市场总览 ──
    ud = data.get("涨跌", {})
    amt = data.get("成交额", 0)
    if ud:
        total = ud["上涨"] + ud["下跌"] + ud["平盘"]
        lines.append(f"\n**📈 市场总览**")
        lines.append(f'> 成交额 <font color="comment">{_fmt_amt(amt)}</font>　'
                     f'上涨 <font color="warning">{ud["上涨"]}</font> 家　'
                     f'下跌 <font color="info">{ud["下跌"]}</font> 家　'
                     f'上涨比例 {ud["上涨比例"]}%')
        if data.get("均价涨幅"):
            lines.append(f'> 全市场均价涨幅 {_fmt_pct(data["均价涨幅"])}　均换手率 {data["均换手率"]:.2f}%')

    # ── AI 分析（核心） ──
    if ai_analysis:
        lines.append(f"\n> ━━━━ **🤖 AI 盘面分析** ━━━━")
        for aline in ai_analysis.strip().split("\n"):
            aline = aline.strip()
            if aline:
                if aline.startswith("**") and "**" in aline[2:]:
                    parts2 = aline.split("**")
                    lines.append(f'\n**{parts2[1]}**{parts2[2] if len(parts2) > 2 else ""}')
                else:
                    lines.append(aline)
        lines.append("> ━━━━━━━━━━━━━━━━━━━━")

    # ── 资金流向 ──
    inflows = data.get("净流入TOP", [])
    outflows = data.get("净流出TOP", [])
    if inflows or outflows:
        lines.append(f"\n**💰 主力资金**")
        if inflows:
            # 过滤掉负数值（净流出的不应该出现在净流入列表）
            valid_inflows = [(sn, amt) for sn, amt in inflows if amt > 0]
            if valid_inflows:
                items = ", ".join([f'{sn}(<font color="warning">{_fmt_amt(amt)}</font>)' for sn, amt in valid_inflows[:5]])
                lines.append(f'> 净流入: {items}')
        if outflows:
            # 过滤掉正数值（净流入的不应该出现在净流出列表）
            valid_outflows = [(sn, amt) for sn, amt in outflows if amt < 0]
            if valid_outflows:
                items = ", ".join([f'{sn}(<font color="info">{_fmt_amt(abs(amt))}</font>)' for sn, amt in valid_outflows[:5]])
                lines.append(f'> 净流出: {items}')

    # ── 热门概念 ──
    concepts = data.get("热门概念", [])
    if concepts:
        items = []
        for r in concepts[:8]:
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg) > 0 else "info"
            items.append(f'<font color="{color}">{r["concept_name"]}({_fmt_pct(float(chg))})</font>')
        lines.append(f"\n**🔥 热门概念**  {' · '.join(items)}")

    # ── 热门行业 ──
    industries = data.get("热门行业", [])
    if industries:
        items = []
        for r in industries[:6]:
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg) > 0 else "info"
            items.append(f'<font color="{color}">{r["concept_name"]}({_fmt_pct(float(chg))})</font>')
        lines.append(f"\n**🏭 热门行业**  {' · '.join(items)}")

    # ── 热门个股 ──
    hot_stocks = data.get("热门个股", [])
    if hot_stocks:
        lines.append(f"\n**⭐ 融合热门个股 TOP{len(hot_stocks)}**")
        items = []
        for r in hot_stocks[:10]:
            chg = r.get("change_pct") or 0
            name = r.get("short_name", "") or r.get("stock_code", "")
            code = r.get("stock_code", "")
            tag = r.get("pop_tag", "") or r.get("concept_tag", "") or ""
            color = "warning" if float(chg or 0) > 0 else "info"
            items.append(f'<font color="{color}">{name}({code}) {_fmt_pct(float(chg))}</font>{(" "+tag) if tag else ""}')
        lines.append("> " + "\n> ".join(items))

    # ── 龙虎榜 ──
    lhb = data.get("龙虎榜", [])
    if lhb:
        lines.append(f"\n**🐲 龙虎榜**")
        items = []
        for r in lhb[:6]:
            amt = float(r.get("a_net_amount") or 0)
            name = r.get("short_name", "") or r.get("stock_code", "")
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg or 0) > 0 else "info"
            items.append(f'<font color="{color}">{name} 净{_fmt_amt(amt)} ({_fmt_pct(float(chg))})</font>')
        lines.append("> " + "\n> ".join(items))

    # ── 涨跌停 ──
    zt = data.get("涨停板", [])
    dt2 = data.get("跌停板", [])
    if zt:
        names = [r.get("short_name", "") or r["stock_code"] for r in zt[:5]]
        lines.append(f'\n**📈 涨停板** <font color="warning">{", ".join(names)}</font>')
    if dt2:
        names = [r.get("short_name", "") or r["stock_code"] for r in dt2[:5]]
        lines.append(f'\n**📉 跌停板** <font color="info">{", ".join(names)}</font>')

    lines.append(f'\n<font color="comment">数据基于 {date_str} 收盘 · ProBigA 智能晚报 · 仅供参考不构成投资建议</font>')

    return "\n".join(lines)


def push_to_wecom(content: str) -> bool:
    """推送到早报机器人"""
    MAX_BYTES = 4000
    content_bytes = content.encode("utf-8")

    if len(content_bytes) <= MAX_BYTES:
        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        try:
            r = httpx.post(WX_BRIEFING_URL, json=payload, timeout=15)
            resp = r.json()
            if resp.get("errcode") == 0:
                log.info("推送成功 (%d 字节)", len(content_bytes))
                return True
            log.warning("推送失败: %s", resp)
            return False
        except Exception as e:
            log.error("推送异常: %s", e)
            return False

    # 超长分段
    paragraphs = content.split("\n\n")
    parts, cur = [], ""
    for para in paragraphs:
        candidate = (cur + "\n\n" + para).strip() if cur else para
        if len(candidate.encode("utf-8")) > MAX_BYTES:
            if cur:
                parts.append(cur)
                cur = para
            else:
                for line in para.split("\n"):
                    if cur and len((cur + "\n" + line).encode("utf-8")) > MAX_BYTES:
                        parts.append(cur)
                        cur = line
                    else:
                        cur = (cur + "\n" + line).strip()
                cur = ""
        else:
            cur = candidate
    if cur:
        parts.append(cur)

    n = len(parts)
    ok = 0
    for i, part in enumerate(parts):
        header = f"## 📊 晚报 ({i+1}/{n})\n\n"
        payload = {"msgtype": "markdown", "markdown": {"content": header + part}}
        try:
            r = httpx.post(WX_BRIEFING_URL, json=payload, timeout=15)
            if r.json().get("errcode") == 0:
                ok += 1
                log.info("  第 %d/%d 段 ✓", i + 1, n)
            else:
                log.warning("  第 %d/%d 段失败: %s", i + 1, n, r.text[:100])
        except Exception as e:
            log.error("  第 %d/%d 段异常: %s", i + 1, n, e)
        if i < n - 1:
            time.sleep(2)
    return ok == n


def _query(engine, sql: str, params: dict = None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]


def _find_latest_trade_date(engine) -> str:
    rows = _query(engine, "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type = 1")
    if rows and rows[0].get("d"):
        return str(rows[0]["d"])
    return datetime.now().strftime("%Y-%m-%d")


def analyze_with_deepseek(data: dict, date_str: str) -> str:
    """用 DeepSeek 对盘面数据进行 AI 复盘分析"""
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        env_file = ROOT / ".env"
        if env_file.is_file():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("DEEPSEEK_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not api_key:
        log.warning("未配置 DEEPSEEK_API_KEY，跳过 AI 分析")
        return ""

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")

    # Build prompt
    prompt_parts = [f"## A股盘面数据 ({date_display}收盘)\n"]

    idx = data.get("指数", {})
    if idx:
        idx_lines = ["**主要指数表现**:"]
        for name, r in idx.items():
            idx_lines.append(f"- {name}: {_fmt_pct(r.get('change_pct'))}")
        prompt_parts.append("\n".join(idx_lines))

    ud = data.get("涨跌", {})
    amt = data.get("成交额", 0)
    if ud:
        prompt_parts.append(f"\n**涨跌家数**: 上涨{ud.get('上涨',0)}家, 下跌{ud.get('下跌',0)}家, 平盘{ud.get('平盘',0)}家")
        prompt_parts.append(f"**成交额**: {_fmt_amt(amt)}")
        prompt_parts.append(f"**均价涨幅**: {_fmt_pct(data.get('均价涨幅'))}")

    inflows = data.get("净流入TOP", [])
    outflows = data.get("净流出TOP", [])
    if inflows:
        items = [f"{sn}({_fmt_amt(flow_amt)})" for sn, flow_amt in inflows[:5]]
        prompt_parts.append(f"\n**主力净流入TOP5**: {', '.join(items)}")
    if outflows:
        items = [f"{sn}({_fmt_amt(abs(flow_amt))})" for sn, flow_amt in outflows[:5]]
        prompt_parts.append(f"**主力净流出TOP5**: {', '.join(items)}")

    concepts = data.get("热门概念", [])
    if concepts:
        items = [f'{r["concept_name"]}({_fmt_pct(float(r.get("change_pct") or 0))})' for r in concepts[:8]]
        prompt_parts.append(f"\n**热门概念**: {', '.join(items)}")

    industries = data.get("热门行业", [])
    if industries:
        items = [f'{r["concept_name"]}({_fmt_pct(float(r.get("change_pct") or 0))})' for r in industries[:6]]
        prompt_parts.append(f"**热门行业**: {', '.join(items)}")

    hot_stocks = data.get("热门个股", [])
    if hot_stocks:
        items = []
        for r in hot_stocks[:10]:
            chg = r.get("change_pct") or 0
            name = r.get("short_name", "") or r.get("stock_code", "")
            tag = r.get("pop_tag", "") or r.get("concept_tag", "") or ""
            items.append(f'{name}({_fmt_pct(float(chg))}){(" "+tag) if tag else ""}')
        prompt_parts.append(f"\n**融合热门个股TOP10**: {', '.join(items)}")

    lhb = data.get("龙虎榜", [])
    if lhb:
        items = [f'{r.get("short_name","")}(净{_fmt_amt(float(r.get("a_net_amount") or 0))})' for r in lhb[:6]]
        prompt_parts.append(f"\n**龙虎榜**: {', '.join(items)}")

    zt = data.get("涨停板", [])
    dt2 = data.get("跌停板", [])
    if zt:
        names = [r.get("short_name", "") or r["stock_code"] for r in zt[:5]]
        prompt_parts.append(f"\n**涨停板**: {', '.join(names)}")
    if dt2:
        names = [r.get("short_name", "") or r["stock_code"] for r in dt2[:5]]
        prompt_parts.append(f"**跌停板**: {', '.join(names)}")

    prompt_parts.append(f"""
\n## 分析要求
你是资深A股分析师，请基于以上数据，对今日盘面进行全面复盘分析，控制在400字以内，格式如下：

**📌 大盘总评**（一句话总结今日市场特征）
**📈 趋势研判**（判断市场趋势，多空力量对比，短期方向）
**💰 资金信号**（资金流向释放什么信号？主力偏好哪些方向？）
**🔥 板块轮动**（热门板块涨跌逻辑，轮动特征）
**⚠️ 风险提示**（当前最需关注的1-2个风险点）
**🎯 明日看点**（明天重点关注什么）""")

    try:
        resp = httpx.post(
            DEEPSEEK_URL,
            json={"model": "deepseek-chat", "messages": [
                {"role": "system", "content": "你是资深A股首席分析师，擅长大盘复盘和趋势研判。用简洁专业的语言，给出明确的分析结论。控制在400字以内。"},
                {"role": "user", "content": "\n".join(prompt_parts)},
            ], "temperature": 0.6, "max_tokens": 800},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        log.error("DeepSeek 调用失败: %s", resp.status_code)
        return ""
    except Exception as e:
        log.error("DeepSeek 异常: %s", e)
        return ""


def pro_review_to_wecom(pro_text: str, date_str: str) -> str:
    """将专业复盘Markdown转为企微markdown格式（加颜色标签）"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")

    lines = []
    for line in pro_text.split("\n"):
        # 标题行
        if line.startswith("【") and line.endswith("】"):
            lines.append(f"## 📊 {line}")
        # 数字标题
        elif line and line[0].isdigit() and ". " in line[:4]:
            lines.append(f"\n**{line}**")
        # 列表项
        elif line.startswith("- "):
            content = line[2:]
            # 给关键数据加颜色
            content = _colorize_pro_line(content)
            lines.append(f"> {content}")
        # 空行
        elif not line.strip():
            lines.append("")
        # 其他
        else:
            lines.append(line)

    result = "\n".join(lines)
    result += f'\n\n<font color="comment">数据基于 {date_str} 收盘 · ProBigA 专业复盘 · 仅供参考不构成投资建议</font>'
    return result


def _colorize_pro_line(content: str) -> str:
    """给专业复盘行中的关键数据加企微颜色标签"""
    import re
    # 涨幅数字 +XX% → 红色
    content = re.sub(r'(\+\d+\.?\d*%)', r'<font color="warning">\1</font>', content)
    # 跌幅数字 -XX% → 绿色
    content = re.sub(r'(-\d+\.?\d*%)', r'<font color="info">\1</font>', content)
    # 温度分数 XX 分 → 蓝色
    content = re.sub(r'(\d+\.?\d*)\s*分', r'<font color="comment">\1分</font>', content)
    # "强势"/"偏强" → 红色, "弱势"/"偏弱" → 绿色
    content = content.replace('"强势"', '<font color="warning">"强势"</font>')
    content = content.replace('"偏强"', '<font color="warning">"偏强"</font>')
    content = content.replace('"弱势"', '<font color="info">"弱势"</font>')
    content = content.replace('"偏弱"', '<font color="info">"偏弱"</font>')
    content = content.replace('"冰点"', '<font color="info">"冰点"</font>')
    content = content.replace('"发酵"', '<font color="warning">"发酵"</font>')
    content = content.replace('"高潮"', '<font color="warning">"高潮"</font>')
    content = content.replace('"退潮"', '<font color="info">"退潮"</font>')
    return content


def main():
    p = argparse.ArgumentParser(description="A股每日晚报")
    p.add_argument("date", nargs="?", help="日期 YYYY-MM-DD，默认最新交易日")
    p.add_argument("--test", action="store_true", help="不推送，仅打印")
    p.add_argument("--legacy", action="store_true", help="使用旧版AI分析模式")
    args = p.parse_args()

    engine = get_engine()

    if args.date:
        date_str = args.date
    else:
        date_str = _find_latest_trade_date(engine)
        log.info("使用最新交易日: %s", date_str)

    report = ""

    if not args.legacy:
        # ── 优先使用专业复盘 ──
        log.info("生成专业复盘...")
        try:
            from biz.review.generate import generate_pro_review
            pro_text = generate_pro_review(engine, date_str)
            log.info("专业复盘生成完成 (%d 字)", len(pro_text))
            report = pro_review_to_wecom(pro_text, date_str)
        except Exception as e:
            log.error("专业复盘生成失败: %s，回退到AI分析模式", e)

    if not report:
        # ── 回退：旧版AI分析模式 ──
        log.info("采集盘面数据...")
        data = collect_market_data(engine, date_str)

        log.info("DeepSeek AI 盘面分析中...")
        ai_analysis = analyze_with_deepseek(data, date_str)
        if ai_analysis:
            log.info("AI 分析 %d 字", len(ai_analysis))

        log.info("生成晚报报告...")
        report = build_report(data, date_str, ai_analysis)

    if args.test:
        print(report)
        return

    log.info("推送到企微...")
    push_to_wecom(report)


if __name__ == "__main__":
    main()
