#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股每日早报 — DeepSeek AI 分析 + 多源快讯 + 市场数据

所需环境变量:
  DEEPSEEK_API_KEY    DeepSeek API密钥 (https://platform.deepseek.com)
  MYSQL_URL           数据库地址

执行:
  python -m biz.early_briefing.generate          # → 直接推送到企业微信
  python -m biz.early_briefing.generate --test   # → 仅打印，不推送
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("briefing")

from server.common.batch_db import create_batch_engine, read_records
from server.common.config import get_settings, get_wecom_webhook
from server.common.tech_risk import append_tech_risk_markdown, fetch_tech_risk_signal
from integrations.wecom.delivery import deliver_markdown
try:
    from biz.research_radar.radar import build_research_radar, format_radar_markdown
except Exception:
    build_research_radar = None
    format_radar_markdown = None

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

HEADERS_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def get_engine():
    return create_batch_engine()


def _read_sql(engine, sql: str, params: dict = None) -> list[dict]:
    return read_records(sql, engine, params=params, ignore_errors=True)


def _fmt_pct(v) -> str:
    if v is None: return "-"
    v = float(v); return f"{'+' if v >= 0 else ''}{v:.2f}%"


def _fmt_money(v) -> str:
    if v is None or v == 0: return "-"
    v = float(v)
    if abs(v) >= 1e8: return f"{v / 1e8:.1f}亿"
    if abs(v) >= 1e4: return f"{v / 1e4:.0f}万"
    return f"{v:.0f}"


# ═══════════════════════════════════════════
# 1. 市场数据采集
# ═══════════════════════════════════════════

def collect_market_data(engine) -> dict:
    """采集所有市场数据用于 AI 分析"""
    data = {}

    # 1.1 美股
    try:
        import akshare as ak
        df = ak.stock_us_spot_em()
        if df is not None and not df.empty:
            targets = {".IXIC": "nasdaq", ".INX": "sp500", ".DJI": "dow",
                       "NVDA": "nvda", "AAPL": "aapl", "MSFT": "msft", "TSLA": "tsla",
                       "AMD": "amd", "AVGO": "avgo", "GOOGL": "googl", "AMZN": "amzn"}
            us = {}
            for _, r in df.iterrows():
                c = str(r.get("代码", "")).strip()
                if c in targets:
                    us[targets[c]] = f"{_fmt_pct(r.get('涨跌幅'))} (${r.get('最新价')})"
            data["美股"] = us
    except Exception as e:
        log.warning("美股: %s", e)
        data["美股"] = {"数据状态": f"暂不可用：{e}"}

    # 1.2 A股指数
    rows = _read_sql(engine, """SELECT i.index_code, i.price, i.change_pct FROM sm_index_current i WHERE i.index_code IN
        ('000001','399001','399006','000688','000300') ORDER BY FIELD(i.index_code,'000001','399001','399006','000688','000300')""")
    nm = {"000001": "上证", "399001": "深成指", "399006": "创业板", "000688": "科创50", "000300": "沪深300"}
    data["A股指数"] = {nm.get(r["index_code"], r["index_code"]): f"{_fmt_pct(r.get('change_pct'))} ({r.get('price')})"
                       for r in rows if r.get("price")}

    # 1.3 热门板块（基于最新交易日）
    latest_trade_date = _read_sql(engine, "SELECT MAX(trade_date) AS d FROM sm_stock_kline WHERE k_type=1")
    trade_date = latest_trade_date[0]["d"] if latest_trade_date and latest_trade_date[0].get("d") else None

    rows = _read_sql(engine, "SELECT * FROM st_hot_concept_ths_daily WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_concept_ths_daily WHERE snapshot_date >= :d) ORDER BY plate_type, `rank` LIMIT 20", {"d": trade_date})
    concepts = [f"{r['concept_name']}({_fmt_pct(r.get('change_pct'))})" for r in rows if r.get("plate_type") == 1][:8]
    industries = [f"{r['concept_name']}({_fmt_pct(r.get('change_pct'))})" for r in rows if r.get("plate_type") == 2][:5]
    data["热门概念"] = concepts
    data["热门行业"] = industries

    # 1.4 资金流向
    rows = _read_sql(engine, """SELECT f.stock_code, COALESCE(s.short_name,'') AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f LEFT JOIN si_all_code s ON f.stock_code=s.stock_code
        WHERE f.trade_date = (SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily)
        ORDER BY f.main_net_inflow DESC LIMIT 8""")
    data["净流入TOP"] = [f"{r['sn']}({_fmt_money(r['main_net_inflow'])})" for r in rows if float(r['main_net_inflow'] or 0) > 0]

    rows = _read_sql(engine, """SELECT f.stock_code, COALESCE(s.short_name,'') AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f LEFT JOIN si_all_code s ON f.stock_code=s.stock_code
        WHERE f.trade_date = (SELECT MAX(trade_date) FROM sm_stock_capital_flow_daily)
        ORDER BY f.main_net_inflow ASC LIMIT 5""")
    data["净流出TOP"] = [f"{r['sn']}({_fmt_money(abs(float(r['main_net_inflow'] or 0)))})" for r in rows if float(r['main_net_inflow'] or 0) < 0]

    # 1.5 成交额（基于最新交易日）
    if trade_date:
        rows = _read_sql(engine, "SELECT COALESCE(SUM(amount),0) AS a FROM sm_stock_kline WHERE k_type=1 AND trade_date=:d", {"d": trade_date})
        if rows and rows[0].get("a"):
            data["成交额"] = f"{float(rows[0]['a']) / 1e8:.0f}亿"

    # 1.6 热门个股（基于最新交易日）
    if trade_date:
        rows = _read_sql(engine, """SELECT f.*, t.pop_tag, t.concept_tag FROM st_hot_rank_fused f
            LEFT JOIN st_hot_rank_ths t ON t.stock_code=f.stock_code COLLATE utf8mb4_unicode_ci AND t.snapshot_date=f.snapshot_date
            WHERE f.snapshot_date=(SELECT MAX(snapshot_date) FROM st_hot_rank_fused WHERE snapshot_date >= :d) ORDER BY f.fused_rank LIMIT 10""", {"d": trade_date})
        data["热门个股"] = [f"{r['short_name']}({r['stock_code']}) {_fmt_pct(r.get('change_pct'))} {r.get('pop_tag','') or r.get('concept_tag','')}"
                           for r in rows]

    # 1.7 数据库快讯（近2天重要）
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = _read_sql(engine, """SELECT source, title, content FROM st_news_flash
        WHERE DATE(publish_time) >= :d ORDER BY is_top DESC, publish_time DESC LIMIT 30""", {"d": yesterday})
    data["DB快讯"] = [f"[{r.get('source','')}] {r.get('title','') or r.get('content','')}" for r in rows]
    if build_research_radar is not None:
        data["研报趋势雷达"] = build_research_radar(engine, str(trade_date)[:10] if trade_date else None)
    else:
        data["研报趋势雷达"] = {}
    data["风险机会决策雷达"] = fetch_tech_risk_signal(
        lambda sql, params=None: _read_sql(engine, sql, params),
        str(trade_date)[:10] if trade_date else None,
    )
    data["科技风险雷达"] = data["风险机会决策雷达"]

    return data


# ═══════════════════════════════════════════
# 2. 新闻爬取
# ═══════════════════════════════════════════

def crawl_sina_news() -> list[str]:
    items = []
    try:
        url = "https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&num=30"
        resp = httpx.get(url, headers=HEADERS_UA, timeout=15)
        for item in resp.json().get("result", {}).get("data", [])[:20]:
            t = (item.get("title") or "")[:120]
            if t and len(t) > 10:
                items.append(f"[新浪] {t}")
    except Exception as e:
        log.warning("新浪: %s", e)
    return items


def crawl_cls_telegraph() -> list[str]:
    items = []
    try:
        url = "https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=8.4.6"
        resp = httpx.get(url, headers={**HEADERS_UA, "Referer": "https://www.cls.cn/telegraph"}, timeout=15)
        data = resp.json()
        for item in data.get("data", {}).get("roll_data", [])[:20]:
            t = (item.get("title") or item.get("content") or "")[:120]
            if t and len(t) > 10:
                items.append(f"[财联社] {t}")
    except Exception as e:
        log.warning("财联社: %s", e)
        # fallback
        try:
            url = "https://www.cls.cn/api/telegraph/list?category=all&limit=20"
            resp = httpx.get(url, headers={**HEADERS_UA, "Referer": "https://www.cls.cn/telegraph"}, timeout=15)
            for item in resp.json().get("data", {}).get("roll_data", [])[:15]:
                t = (item.get("title") or item.get("content") or "")[:120]
                if t and len(t) > 10:
                    items.append(f"[财联社] {t}")
        except Exception:
            log.debug("Failed to crawl CLS headlines.", exc_info=True)
    return items


def crawl_eastmoney_headlines() -> list[str]:
    items = []
    try:
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&fields=f12,f14,f2,f3&secids=1.000001,0.399001,0.399006"
        resp = httpx.get(url, timeout=10)
        d = resp.json().get("data", {}).get("diff", [])
        for r in d:
            items.append(f"[东方财富] {r.get('f14','')} 最新价{r.get('f2','')} 涨跌幅{_fmt_pct(r.get('f3'))}")
    except Exception as e:
        log.warning("东财首页: %s", e)
    return items


def crawl_all_news() -> list[str]:
    all_items = []
    all_items.extend(crawl_sina_news())
    all_items.extend(crawl_cls_telegraph())
    all_items.extend(crawl_eastmoney_headlines())
    seen = set()
    deduped = []
    for item in all_items:
        key = item[:50]
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped[:50]


# ═══════════════════════════════════════════
# 3. DeepSeek AI 分析
# ═══════════════════════════════════════════

def call_deepseek(messages: list[dict]) -> str:
    settings = get_settings()
    api_key = (settings.deepseek_api_key or "").strip()
    if not api_key:
        raise RuntimeError("未配置 DEEPSEEK_API_KEY，请在 .env 中设置")

    resp = httpx.post(
        DEEPSEEK_URL,
        json={"model": settings.deepseek_model, "messages": messages, "temperature": 0.7, "max_tokens": 8000},
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=180,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"DeepSeek 返回 {resp.status_code}: {resp.text[:300]}")
    return resp.json()["choices"][0]["message"]["content"]


def build_system_prompt() -> str:
    return """你是一个顶级A股市场分析师，每天早上为专业投资者撰写深度「A股盘前早报」。

你的分析框架（必须覆盖全部）：
1. **隔夜美股深度映射**：不仅看三大指数涨跌，更要分析VIX、美债收益率、美元指数对A股北向资金和风险偏好的传导链
2. **产业链逻辑完整推演**：从上游原材料→中游制造→下游应用的完整链条，标注具体的受益标的和替代关系
3. **资金行为双重验证**：结合主力资金流向+龙虎榜+两融数据，判断资金态度是否与消息面一致
4. **机构行为研判**：关注基金仓位变化、重仓股异动、期指持仓，预判机构调仓方向
5. **情绪指标量化**：涨停家数、连板高度、炸板率、量比等微观情绪信号
6. **多空博弈推演**：针对每个主线，列出多方逻辑和空方逻辑，给出倾向性判断
7. **风险全景扫描**：政策风险、业绩雷、解禁、减持、外围黑天鹅
8. **风险/机会自主判断**：必须综合「风险机会决策雷达」、外围映射、A股涨跌家数、板块强弱、资金流和实际持仓；若 risk.triggered=true，要醒目写出需要先跑/先减仓的板块和命中持仓；若 opportunity.status=focus/watch，要列出机会板块和具体观察个股

写作风格：
- 标题格式："## 📰 A股早报 | X月X日 星期X"
- 每节用 **粗体标题** + emoji图标开头
- 关键数字必须用 **加粗** 高亮，如 **+3.2%**、**16.7亿**
- 逻辑链条用 → 符号串联，如 "上游涨价 → 中游利润压缩 → 下游需求萎缩"
- 结论要有明确的倾向性（看多/看空/中性），不要模棱两可
- 用简洁有力的短句，每段 2-4 句话
- 直接点名具体个股，不要用"某公司"代替

不要编造任何未提供的新闻或数据。基于我给你的数据深入分析，尽量详细、全面。"""


def build_user_prompt(data: dict, news: list[str]) -> str:
    now = datetime.now()
    date_label = f"{now.month}月{now.day}日 星期{['一','二','三','四','五','六','日'][now.weekday()]}"

    prompt = f"""今天是 {date_label}，请基于以下市场数据和新闻，生成一份深度A股盘前早报。

## 市场数据

"""
    for k, v in data.items():
        if isinstance(v, dict):
            prompt += f"**{k}**: {json.dumps(v, ensure_ascii=False)}\n"
        elif isinstance(v, list):
            prompt += f"**{k}**: {', '.join(v[:12])}\n"
        else:
            prompt += f"**{k}**: {v}\n"

    prompt += "\n## 新闻快讯（今天重要消息）\n\n"
    prompt += "\n".join(news[:50])

    prompt += f"""

## 撰写要求

请按以下结构深度分析（每节至少 3-5 段，带具体数据和逻辑链）：

### 📈 隔夜外盘深度
美股三指涨跌 + VIX/美债/美元 + 中概股表现 + 对A股映射（北向资金预期、开盘情绪、板块联动）

### 🗺️ 全市场催化候选池（不得省略）
先完成全市场扫描，再做优先级排序。必须逐项检查：外盘与风险偏好、政策与流动性、供给扰动、涨价、
国产替代/贸易摩擦、订单与资本开支、需求景气、技术/产品发布、财报业绩、并购重组、会议展会和消费事件。
- 凡是能形成“消息催化→产业传导→A股环节/个股映射→盘面验证”的方向都要列出，不得因为排名靠后而删除
- 按 S/A/B/观察/逻辑转弱 分层；没有合格个股的主题仍须保留，并注明“暂无合格标的”
- 对无法归类但重要的消息，单列“未归类高优先级催化”，不能静默丢弃
- 每条写明：催化证据、产业链受益/受损环节、具体标的、盘面验证、证伪条件

### 🎯 今日优先主线
从上述全量候选池中再选 2-3 条最高优先级主线，但这只是交易优先级，不代替也不删减全市场候选池。
每条主线必须包含：
- 逻辑链：从消息→产业链受益环节→具体标的
- 多方逻辑 vs 空方逻辑
- 倾向性判断与今日验证点

### 🧭 研报趋势雷达
必须单独列出这一节，基于“研报趋势雷达”数据说明：
- 博主/公众号热度只是情绪入口，券商/投行研报和财报公告是验证层
- 展示全部已扫描主题及 S/A/B/观察/逻辑转弱排序，不得只截取前几名
- 每条主线对应的核心股票池、验证指标、主要风险
- 做覆盖审计：列出扫描维度数、活跃主题数、逻辑转弱主题数、暂无合格标的主题和未归类催化
- 明确说明这不是买卖建议

### 🔥 板块轮动全景
结合热门概念/行业数据，分析：
- 领涨板块的持续性和逻辑
- 领跌板块的调整原因
- 资金切换方向（从哪流出、流入哪）
- 板块内部分化（龙头 vs 跟风）

### 💰 资金面深度
- 主力资金 TOP 流入/流出 各 5 个
- 龙虎榜分析（如有）
- 北向资金态度研判
- 两融/期指信号

### 📊 情绪与量价
- 涨停/跌停家数、连板高度、炸板率
- 成交额变化趋势
- 涨跌比、量比分析

### ⚠️ 风险全景
分点列出：政策风险、业绩雷、解禁减持、高位回调、外围黑天鹅
如果「风险机会决策雷达」的 risk 触发，必须把“对应板块先跑/先减仓、命中持仓先处理”列为第一风险；同时给出机会侧板块和候选个股，但不能用机会掩盖风险。

### 📌 今日重点观测
列出 5-8 个今日必须盯盘的变量（个股/板块/数据）

请尽量详细深入分析，不要敷衍。基于实际数据，给出明确的倾向性结论。"""

    return prompt


# ═══════════════════════════════════════════
# 4. 推送
# ═══════════════════════════════════════════

def push_to_wecom(content: str, engine=None) -> bool:
    """可靠投递早报；任何缺配置或部分失败都会抛错。"""

    result = deliver_markdown(
        get_wecom_webhook("briefing", required=False),
        content,
        engine=engine or get_engine(),
        delivery_kind="early_briefing",
        webhook_kind="briefing",
        title="## 📰 A股早报",
    )
    log.info(
        "企微推送完成: delivery_id=%s, %d/%d 段",
        result.delivery_id,
        result.delivered_count,
        result.segment_count,
    )
    return result.success


def append_research_radar(content: str, market_data: dict) -> str:
    radar = market_data.get("研报趋势雷达")
    if not radar or format_radar_markdown is None or "研报趋势雷达" in content:
        return content
    return content.rstrip() + "\n\n" + format_radar_markdown(radar, title="🧭 研报趋势雷达")


def append_tech_risk(content: str, market_data: dict) -> str:
    return append_tech_risk_markdown(content, market_data.get("风险机会决策雷达") or market_data.get("科技风险雷达"))


def _safe_print(text_value: str) -> None:
    try:
        print(text_value)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text_value + "\n").encode("utf-8", errors="replace"))


# ═══════════════════════════════════════════
# 5. 主函数
# ═══════════════════════════════════════════

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--test", action="store_true", help="不推送，仅打印")
    p.add_argument("--output", default="")
    args, unknown = p.parse_known_args()

    engine = get_engine()

    # 采集数据
    log.info("1/4 采集市场数据...")
    market_data = collect_market_data(engine)

    log.info("2/4 爬取新闻快讯...")
    news = crawl_all_news()

    # 补充 DB 快讯（去重）
    db_news = market_data.pop("DB快讯", [])
    seen_titles = set()
    all_news = db_news + news
    deduped_news = []
    for n in all_news:
        key = n[:40]
        if key not in seen_titles:
            seen_titles.add(key)
            deduped_news.append(n)
    log.info("  共 %d 条（DB:%d + crawl:%d）", len(deduped_news), len(db_news), len(news))

    # DeepSeek 分析
    api_key = (get_settings().deepseek_api_key or "").strip()
    if args.test:
        briefing = _generate_fallback(market_data, deduped_news)
        log.info("3/4 跳过 AI（--test模式），使用模板生成")
    elif not api_key:
        log.warning("3/4 未配置 DEEPSEEK_API_KEY，使用模板生成")
        briefing = _generate_fallback(market_data, deduped_news)
    else:
        log.info("3/4 DeepSeek AI 分析中...")
        try:
            briefing = call_deepseek([
                {"role": "system", "content": build_system_prompt()},
                {"role": "user", "content": build_user_prompt(market_data, deduped_news)},
            ])
            log.info("  AI 返回 %d 字", len(briefing))
        except Exception as e:
            log.error("DeepSeek 调用失败: %s", e)
            briefing = _generate_fallback(market_data, deduped_news)

    briefing = append_tech_risk(briefing, market_data)
    briefing = append_research_radar(briefing, market_data)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(briefing)

    if args.test:
        _safe_print("\n" + "=" * 60)
        _safe_print(briefing)
        _safe_print("=" * 60)
        return 0

    log.info("4/4 推送到企业微信...")
    push_to_wecom(briefing, engine=engine)
    log.info("完成")
    return 0


def _generate_fallback(market_data: dict, news: list[str]) -> str:
    """无 AI 时的模板生成（应急用）"""
    now = datetime.now()
    date_label = f"{now.month}月{now.day}日 星期{['一','二','三','四','五','六','日'][now.weekday()]}"

    lines = [f"## A股早报 | {date_label}", ""]

    us = market_data.get("美股", {})
    if us:
        lines.append("**📈 美股隔夜**")
        parts = []
        for k in ["nasdaq", "sp500", "dow"]:
            if k in us:
                parts.append(f"{k.upper()} {us[k]}")
        if parts: lines.append(" | ".join(parts))
        techs = []
        for k in ["nvda", "aapl", "msft", "tsla"]:
            if k in us:
                techs.append(f"{k.upper()} {us[k]}")
        if techs: lines.append("科技: " + " ".join(techs))
        if not parts and not techs and us.get("数据状态"):
            lines.append(us["数据状态"])
        lines.append("")

    a_idx = market_data.get("A股指数", {})
    if a_idx:
        lines.append("**📊 A股指数**")
        lines.append(" | ".join(f"{k} {v}" for k, v in a_idx.items()))
        lines.append("")

    concepts = market_data.get("热门概念", [])
    industries = market_data.get("热门行业", [])
    if concepts or industries:
        lines.append("**🔥 热门板块**")
        if concepts: lines.append("概念: " + " ".join(concepts[:8]))
        if industries: lines.append("行业: " + " ".join(industries[:5]))
        lines.append("")

    inflow = market_data.get("净流入TOP", [])
    outflow = market_data.get("净流出TOP", [])
    if inflow or outflow:
        lines.append("**💰 主力资金**")
        if inflow: lines.append("净流入: " + " ".join(inflow[:5]))
        if outflow: lines.append("净流出: " + " ".join(outflow[:5]))
        lines.append("")

    hot_stocks = market_data.get("热门个股", [])
    if hot_stocks:
        lines.append("**⭐ 热门个股**")
        for i, s in enumerate(hot_stocks[:8], 1):
            lines.append(f"{i}. {s}")
        lines.append("")

    if news:
        lines.append("**📢 热点资讯**")
        for n in news[:8]:
            lines.append(f"- {n[:100]}")
        lines.append("")

    decision_radar = append_tech_risk_markdown("", market_data.get("风险机会决策雷达") or market_data.get("科技风险雷达")).strip()
    if decision_radar:
        lines.append(decision_radar)
        lines.append("")

    radar = market_data.get("研报趋势雷达")
    if radar and format_radar_markdown is not None:
        lines.append(format_radar_markdown(radar, title="🧭 研报趋势雷达"))
        lines.append("")

    amt = market_data.get("成交额", "暂无")
    lines.append(f"**📊 两市成交额: {amt}**")
    lines.append("")
    lines.append("> ⚠️ 以上为AI生成的盘前参考，不构成投资建议")
    lines.append("> 如需启用DeepSeek AI分析，请配置 DEEPSEEK_API_KEY")

    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
