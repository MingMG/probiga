# -*- coding: utf-8 -*-
"""多源快讯定时同步 + 企业微信推送（实时/每日早报/每周前瞻）"""
import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import httpx
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
WX_NEWS_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ba832a31-8ab8-4981-8491-42cf4650cddd"
WX_BRIEFING_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b1110965-119d-438b-856d-0d87c751cf13"

IMPORTANT_KEYWORDS = [
    "央行", "降息", "加息", "降准", "LPR", "MLF", "逆回购", "公开市场",
    "中美", "关税", "贸易", "制裁", "谈判",
    "国务院", "证监会", "发改委", "财政部", "工信部", "国新办",
    "GDP", "CPI", "PPI", "PMI", "社融", "M2", "经济数据",
    "战争", "冲突", "封锁", "军事", "导弹",
    "涨停", "跌停", "熔断", "暴涨", "暴跌",
    "退市", "ST", "暴雷", "违约", "破产",
    "利率", "汇率", "外汇", "人民币",
    "重大", "突发", "紧急", "独家",
    "会议", "峰会", "论坛", "发布会", "签约", "开幕",
    "新能源", "芯片", "半导体", "AI", "人工智能",
]

MARKET_IMPACT_HIGH = [
    "降息", "降准", "加息", "LPR", "MLF", "熔断", "暴涨", "暴跌",
    "突发", "紧急", "暂停交易", "紧急停牌", "全面降准", "定向降准",
    "涨停潮", "跌停潮", "千股跌停", "千股涨停",
]
MARKET_IMPACT_MED = [
    "央行", "证监会", "国务院", "发改委", "财政部",
    "中美", "关税", "制裁", "贸易谈判",
    "战争", "冲突", "封锁", "军事",
    "退市", "ST", "暴雷", "违约", "破产",
    "PMI", "GDP", "CPI", "社融", "M2",
    "人民币", "汇率",
]

SRC_LABELS = {"cls": "财联社", "eastmoney": "东方财富", "sina": "新浪财经"}
SRC_COLORS = {"cls": "warning", "eastmoney": "info", "sina": "comment"}


def get_engine():
    url = os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)
    return create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=2)


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
    text_to_check = ((it.get("title") or "") + (it.get("content") or ""))
    text_lower = text_to_check.lower()
    for kw in IMPORTANT_KEYWORDS:
        if kw.lower() in text_lower:
            score += 1
            break
    for kw in MARKET_IMPACT_HIGH:
        if kw in text_to_check:
            score += 3
            break
    else:
        for kw in MARKET_IMPACT_MED:
            if kw in text_to_check:
                score += 1
                break
    if it.get("stocks"):
        score += 1
    return score


def _categorize_news(items: list) -> dict:
    categories = {
        "政策监管": ["央行", "国务院", "证监会", "发改委", "财政部", "工信部", "国新办",
                       "降息", "加息", "降准", "LPR", "MLF", "逆回购", "监管", "政策", "法规"],
        "宏观经济": ["GDP", "CPI", "PPI", "PMI", "社融", "M2", "经济数据", "进出口",
                       "贸易", "关税", "制裁", "中美", "汇率", "人民币", "利率"],
        "市场异动": ["涨停", "跌停", "熔断", "暴涨", "暴跌", "退市", "ST", "暴雷",
                       "违约", "破产", "IPO", "回购"],
        "行业科技": ["新能源", "芯片", "半导体", "AI", "人工智能", "光伏", "锂电",
                       "华为", "特斯拉", "比亚迪", "算力", "机器人"],
        "国际局势": ["战争", "冲突", "封锁", "军事", "导弹", "石油", "OPEC",
                       "美联储", "欧央行", "日央行"],
        "会议活动": ["会议", "峰会", "论坛", "发布会", "签约", "开幕", "博览会",
                       "大会", "展览", "圆桌"],
    }
    result = {cat: [] for cat in categories}
    result["其他"] = []
    for it in items:
        text_check = ((it.get("title") or "") + (it.get("content") or ""))
        matched = False
        for cat_name, keywords in categories.items():
            for kw in keywords:
                if kw in text_check:
                    result[cat_name].append(it)
                    matched = True
                    break
            if matched:
                break
        if not matched:
            result["其他"].append(it)
    return {k: v for k, v in result.items() if v}


def fetch_cls(client, pages=2):
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
            content_clean = re.sub(r"<[^>]+>", "", content)
            ts = it.get("ctime") or it.get("modified_time")
            dt_obj = None
            time_str = ""
            if ts:
                try:
                    dt_obj = datetime.fromtimestamp(int(ts))
                    time_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    time_str = str(ts)
            subjects = it.get("subjects") or []
            items.append({
                "source": "cls", "source_id": str(it.get("id") or ""),
                "title": it.get("title") or "", "content": content_clean[:800],
                "time": time_str, "publish_time": dt_obj,
                "level": it.get("level") or "C",
                "subjects": [{"name": s.get("subject_name", "")} for s in subjects[:5]],
                "stocks": stock_info, "reading_num": it.get("reading_num") or 0,
                "is_top": bool(it.get("is_top")), "jpush": bool(it.get("jpush")),
                "bold": bool(it.get("bold")), "author": it.get("author") or "",
            })
        last_time = roll_data[-1].get("ctime") or 0
        if not last_time:
            break
    return items


def fetch_eastmoney(client, pages=1):
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
                try:
                    dt_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
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
                "source": "eastmoney", "source_id": str(it.get("code") or ""),
                "title": it.get("title") or "", "content": it.get("summary") or "",
                "time": ts_str, "publish_time": dt_obj, "level": "C",
                "subjects": [], "stocks": stock_info, "reading_num": 0,
                "is_top": False, "jpush": False, "bold": False, "author": "东方财富",
            })
    return items


def fetch_sina(client, pages=1):
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
                try:
                    dt_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
            tag_list = it.get("tag") or []
            subjects = [{"name": t.get("name", "")} for t in tag_list if t.get("name")]
            stock_info = []
            ext_raw = it.get("ext") or ""
            if ext_raw:
                try:
                    ext = json.loads(ext_raw) if isinstance(ext_raw, str) else ext_raw
                    for s in (ext.get("stocks") or []):
                        if isinstance(s, dict) and s.get("symbol"):
                            stock_info.append({"name": s.get("key", ""), "code": s.get("symbol", "")})
                except Exception:
                    pass
            items.append({
                "source": "sina", "source_id": str(it.get("id") or ""),
                "title": "", "content": it.get("rich_text") or "",
                "time": ts_str, "publish_time": dt_obj, "level": "C",
                "subjects": subjects, "stocks": stock_info, "reading_num": 0,
                "is_top": bool(it.get("top_value")), "jpush": False, "bold": False,
                "author": "新浪财经",
            })
    return items


def save_to_db(engine, items):
    etl = datetime.now().replace(microsecond=0)
    upsert = text(
        "INSERT INTO st_news_flash (source, source_id, title, content, publish_time, level, stocks, subjects, reading_num, is_top, jpush, extra, pushed, etl_sync_at) "
        "VALUES (:source, :source_id, :title, :content, :publish_time, :level, :stocks, :subjects, :reading_num, :is_top, :jpush, :extra, 0, :etl_sync_at) "
        "ON DUPLICATE KEY UPDATE title=VALUES(title), content=VALUES(content), level=VALUES(level), reading_num=VALUES(reading_num), etl_sync_at=VALUES(etl_sync_at)"
    )
    saved = 0
    with engine.begin() as conn:
        for it in items:
            try:
                conn.execute(upsert, {
                    "source": it["source"], "source_id": it["source_id"],
                    "title": (it.get("title") or "")[:512], "content": it.get("content") or "",
                    "publish_time": it.get("publish_time"), "level": it.get("level") or "C",
                    "stocks": json.dumps(it.get("stocks") or [], ensure_ascii=False),
                    "subjects": json.dumps(it.get("subjects") or [], ensure_ascii=False),
                    "reading_num": it.get("reading_num") or 0,
                    "is_top": 1 if it.get("is_top") else 0,
                    "jpush": 1 if it.get("jpush") else 0,
                    "extra": None, "etl_sync_at": etl,
                })
                saved += 1
            except Exception:
                pass
    return saved


def push_to_wecom(content_md, webhook_url=None):
    if webhook_url is None:
        webhook_url = WX_NEWS_URL
    payload = {"msgtype": "markdown", "markdown": {"content": content_md}}
    try:
        with httpx.Client(timeout=10) as client:
            r = client.post(webhook_url, json=payload)
            resp = r.json()
            if resp.get("errcode") == 0:
                return True
            log.warning("企业微信推送失败: %s", resp)
            return False
    except Exception as e:
        log.error("企业微信推送异常: %s", e)
        return False


def push_wecom_batched(blocks, header=None, webhook_url=None):
    if webhook_url is None:
        webhook_url = WX_NEWS_URL
    MAX_BYTES = 4000
    messages = []
    current_parts = []
    current_bytes = 0

    if header:
        h_bytes = len(header.encode("utf-8"))
        current_parts.append(header)
        current_bytes += h_bytes

    for block in blocks:
        block_bytes = len(block.encode("utf-8"))
        if current_parts and current_bytes + block_bytes + 1 > MAX_BYTES:
            messages.append("\n".join(current_parts))
            current_parts = []
            current_bytes = 0
        if block_bytes > MAX_BYTES:
            if current_parts:
                messages.append("\n".join(current_parts))
                current_parts = []
                current_bytes = 0
            messages.append(block)
        else:
            current_parts.append(block)
            current_bytes += block_bytes + 1

    if current_parts:
        messages.append("\n".join(current_parts))

    n = len(messages)
    ok_count = 0
    for idx, msg in enumerate(messages):
        if n > 1:
            suffix = f" ({idx + 1}/{n})"
            if msg.startswith("## "):
                nl = msg.index("\n") if "\n" in msg else len(msg)
                msg = msg[:nl] + suffix + msg[nl:]
            else:
                msg = suffix + "\n" + msg

        success = False
        for attempt in range(1, 4):
            if push_to_wecom(msg, webhook_url=webhook_url):
                success = True
                ok_count += 1
                log.info("  分段 %d/%d 推送成功 (%d 字节)", idx + 1, n, len(msg.encode("utf-8")))
                break
            log.warning("  分段 %d/%d 第 %d 次推送失败，%s后重试", idx + 1, n, attempt, "3s" if attempt < 3 else "放弃")
            if attempt < 3:
                time.sleep(3)

        if not success:
            log.error("  分段 %d/%d 全部重试失败 (%d 字节)", idx + 1, n, len(msg.encode("utf-8")))

        if idx < n - 1 and success:
            time.sleep(3)

    return ok_count == n


def _load_rows_as_dicts(result):
    return [dict(zip(result.keys(), row)) for row in result.fetchall()]


def _parse_row(row):
    it = dict(row)
    it["stocks"] = json.loads(row["stocks"]) if row.get("stocks") else []
    it["subjects"] = json.loads(row["subjects"]) if row.get("subjects") else []
    it["title"] = it.get("title") or ""
    it["content"] = it.get("content") or ""
    return it


def _fmt_time_short(dt):
    if dt and hasattr(dt, "strftime"):
        return dt.strftime("%H:%M")
    return ""


def _fmt_src(src):
    return SRC_LABELS.get(src, src)


def _fmt_score_badge(score):
    if score >= 7:
        return f'<font color="warning">🔴 {score}分</font>'
    elif score >= 5:
        return f'<font color="warning">🟠 {score}分</font>'
    elif score >= 3:
        return f'<font color="info">🟡 {score}分</font>'
    else:
        return f'<font color="comment">⚪ {score}分</font>'


def _build_news_line(it, show_score=False):
    time_str = _fmt_time_short(it.get("publish_time"))
    src = _fmt_src(it["source"])
    src_color = SRC_COLORS.get(it["source"], "comment")
    title = it["title"] or ""
    content = it["content"] or ""
    level = it.get("level", "C")
    parts = []
    if time_str:
        parts.append(f'<font color="comment">{time_str}</font>')
    parts.append(f'<font color="{src_color}">{src}</font>')
    if level in ("A", "B"):
        color = "warning" if level == "A" else "info"
        parts.append(f'<font color="{color}">[{level}]</font>')
    header = " | ".join(parts)
    body_parts = []
    if title:
        body_parts.append(f"**{title}**")
    if content and content != title:
        body_parts.append(content)
    body = "\n".join(body_parts) if body_parts else title or content
    extras = []
    if show_score and it.get("importance_score"):
        extras.append(_fmt_score_badge(it["importance_score"]))
    if it.get("stocks"):
        names = [s.get("name", "") for s in it["stocks"][:3] if s.get("name")]
        if names:
            extras.append("📊 " + "、".join(names))
    if it.get("subjects"):
        names = [s.get("name", "") for s in it["subjects"][:3] if s.get("name")]
        if names:
            extras.append("🏷 " + "、".join(names))
    extra_str = ""
    if extras:
        extra_str = "\n> " + " | ".join(extras)
    return f"{header}\n{body}{extra_str}"


def _is_market_moving(it: dict) -> bool:
    text = ((it.get("title") or "") + (it.get("content") or ""))
    if it.get("level") == "A" and it.get("jpush"):
        return True
    for kw in MARKET_IMPACT_HIGH:
        if kw in text:
            return True
    if it.get("level") == "A":
        return True
    for kw in MARKET_IMPACT_MED:
        if kw in text:
            return True
    if it.get("is_top") and it.get("reading_num", 0) >= 10000:
        return True
    return False


# ─────────────────────────────────────────────
# 推送1: 实时重要快讯
# ─────────────────────────────────────────────

def push_important(engine):
    rows = []
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT id, source, title, content, publish_time, level, stocks, subjects, "
            "reading_num, jpush, is_top FROM st_news_flash WHERE pushed = 0 "
            "ORDER BY publish_time DESC LIMIT 200"
        ))
        rows = _load_rows_as_dicts(result)

    if not rows:
        log.info("没有新的未推送快讯")
        return 0

    important = []
    for row in rows:
        it = _parse_row(row)
        score = _calc_importance(it)
        if score >= 4 and _is_market_moving(it):
            it["importance_score"] = score
            important.append(it)

    if not important:
        log.info("没有影响盘面的重要快讯，跳过推送")
        with engine.begin() as conn:
            conn.execute(text("UPDATE st_news_flash SET pushed=1, pushed_at=NOW() WHERE pushed=0"))
        return 0

    important.sort(key=lambda x: x.get("importance_score", 0), reverse=True)
    to_push = important[:5]

    sources_used = sorted(set(it["source"] for it in to_push))
    src_tag = " / ".join(_fmt_src(s) for s in sources_used)
    now_str = datetime.now().strftime("%H:%M")
    sep = "> ━━━━━━━━━━━━━━━━━━━━━━━━"

    header = f"## 🔥 重要快讯 <font color=\"warning\">{now_str}</font>\n<font color=\"comment\">来源: {src_tag} | 共 {len(to_push)} 条</font>"

    blocks = []
    for it in to_push:
        blocks.append(f"{sep}\n{_build_news_line(it, show_score=True)}")

    ok = push_wecom_batched(blocks, header=header)
    if ok:
        pushed_ids = [it["id"] for it in to_push]
        with engine.begin() as conn:
            for pid in pushed_ids:
                conn.execute(text("UPDATE st_news_flash SET pushed=1, pushed_at=NOW() WHERE id=:id"), {"id": pid})
        log.info("推送成功: %d 条重要快讯", len(to_push))
        return len(to_push)
    else:
        log.error("推送失败，不标记为已推送")
        return 0


# ─────────────────────────────────────────────
# 推送2: 每日早报 (8:30 汇总前一天)
# ─────────────────────────────────────────────

def push_daily_briefing(engine):
    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today_str = now.strftime("%m月%d日").replace("月0", "月").replace("日0", "日")

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT id, source, title, content, publish_time, level, stocks, subjects, "
            "reading_num, jpush, is_top FROM st_news_flash "
            "WHERE DATE(publish_time) = :d ORDER BY publish_time DESC"
        ), {"d": yesterday})
        rows = _load_rows_as_dicts(result)

    if not rows:
        log.info("昨日 (%s) 无快讯数据，跳过早报", yesterday)
        return 0

    items = [_parse_row(r) for r in rows]
    for it in items:
        it["importance_score"] = _calc_importance(it)

    important = [it for it in items if it["importance_score"] >= 2]
    important.sort(key=lambda x: x["importance_score"], reverse=True)

    if not important:
        log.info("昨日无重要快讯，跳过早报")
        return 0

    categorized = _categorize_news(important)

    cat_icons = {
        "政策监管": "📋", "宏观经济": "📈", "市场异动": "⚡",
        "行业科技": "🔬", "国际局势": "🌍", "会议活动": "📅", "其他": "📌",
    }

    sep = "> ━━━━━━━━━━━━━━━━━━━━━━━━"

    header = f"## 📊 每日早报 | {today_str}\n<font color=\"comment\">昨日 ({yesterday}) 共 {len(items)} 条快讯，其中重要 {len(important)} 条</font>"

    blocks = []
    total_shown = 0
    max_per_cat = 3
    for cat_name, cat_items in categorized.items():
        if total_shown >= 10:
            break
        icon = cat_icons.get(cat_name, "📌")
        show_count = min(len(cat_items), max_per_cat, 10 - total_shown)
        cat_header = f"**{icon} {cat_name}** ({len(cat_items)}条)"
        for it in cat_items[:show_count]:
            time_str = _fmt_time_short(it.get("publish_time"))
            src = _fmt_src(it["source"])
            src_color = SRC_COLORS.get(it["source"], "comment")
            title = it["title"] or ""
            content = it["content"] or ""
            score = it.get("importance_score", 0)
            line = f'<font color="comment">{time_str}</font> <font color="{src_color}">[{src}]</font> {_fmt_score_badge(score)}'
            if title:
                line += f"\n**{title}**"
            if content and content != title:
                line += f"\n{content}"
            blocks.append(f"{cat_header}\n{sep}\n{line}")
            cat_header = ""
            total_shown += 1

    sources_used = set(it["source"] for it in important)
    src_tag = " / ".join(_fmt_src(s) for s in sorted(sources_used))
    footer = f'<font color="comment">来源: {src_tag} · ProBigA 每日早报</font>'
    blocks.append(footer)

    ok = push_wecom_batched(blocks, header=header, webhook_url=WX_BRIEFING_URL)
    if ok:
        log.info("每日早报推送成功: %d 条重要资讯", len(important))
        return len(important)
    else:
        log.error("每日早报推送失败")
        return 0


# ─────────────────────────────────────────────
# 推送3: 每周前瞻 (周日17:00)
# ─────────────────────────────────────────────

WEEKLY_KEYWORDS = [
    "会议", "峰会", "论坛", "发布会", "签约仪式", "开幕", "博览会",
    "圆桌", "大会", "展览会", "国新办", "新闻发布会",
    "公布", "数据", "PMI", "CPI", "PPI", "社融", "GDP", "M2",
    "LPR", "MLF", "公开市场", "美联储", "利率决议",
    "限售股解禁", "解禁", "IPO", "申购", "上市",
    "财报", "业绩", "季报", "年报", "中报",
]


def push_weekly_preview(engine):
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    next_monday = now + timedelta(days=(7 - now.weekday()) % 7 or 7)
    next_friday = next_monday + timedelta(days=4)
    week_range = f"{next_monday.strftime('%m.%d')}-{next_friday.strftime('%m.%d')}"
    week_range = week_range.replace(".0", ".")

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT id, source, title, content, publish_time, level, stocks, subjects, "
            "reading_num, jpush, is_top FROM st_news_flash "
            "WHERE publish_time >= DATE_SUB(NOW(), INTERVAL 3 DAY) "
            "ORDER BY publish_time DESC LIMIT 500"
        ))
        rows = _load_rows_as_dicts(result)

    all_items = [_parse_row(r) for r in rows]

    weekly_relevant = []
    for it in all_items:
        text_check = (it["title"] + it["content"]).lower()
        matched_kw = []
        for kw in WEEKLY_KEYWORDS:
            if kw.lower() in text_check:
                matched_kw.append(kw)
        if matched_kw:
            it["matched_keywords"] = matched_kw
            it["importance_score"] = _calc_importance(it)
            weekly_relevant.append(it)

    weekly_relevant.sort(key=lambda x: x["importance_score"], reverse=True)

    if not weekly_relevant:
        log.info("无下周相关重要信息，跳过周报")
        return 0

    categorized = _categorize_news(weekly_relevant[:30])

    cat_icons = {
        "政策监管": "📋", "宏观经济": "📈", "市场异动": "⚡",
        "行业科技": "🔬", "国际局势": "🌍", "会议活动": "📅", "其他": "📌",
    }

    sep = "> ━━━━━━━━━━━━━━━━━━━━━━━━"

    header = f"## 📅 下周前瞻 | {week_range}\n<font color=\"comment\">基于近期资讯整理，助你提前布局</font>"

    focus_points = []
    for it in weekly_relevant[:5]:
        title = it["title"] or it["content"][:40]
        kws = it.get("matched_keywords", [])[:3]
        if kws:
            focus_points.append(f"{title}（{'、'.join(kws)}）")
        else:
            focus_points.append(title)

    blocks = []
    if focus_points:
        fp_lines = [sep, "**🔑 重点关注**"]
        for fp in focus_points:
            fp_lines.append(f"> <font color=\"warning\">•</font> {fp}")
        blocks.append("\n".join(fp_lines))

    total_shown = len(focus_points)
    for cat_name, cat_items in categorized.items():
        if cat_name in ("政策监管", "宏观经济", "会议活动", "其他"):
            continue
        if total_shown >= 12:
            break
        icon = cat_icons.get(cat_name, "📌")
        show_count = min(3, 12 - total_shown, len(cat_items))
        if show_count == 0:
            continue
        for it in cat_items[:show_count]:
            title = it["title"] or ""
            content = it["content"] or ""
            kws = it.get("matched_keywords", [])
            kw_tag = ""
            if kws:
                kw_tag = f"\n<font color=\"comment\">#{' #'.join(kws[:2])}</font>"
            part = f"**{icon} {cat_name}动态**\n{sep}\n"
            if title:
                part += f"**{title}**"
            if content and content != title:
                part += f"\n{content}"
            if kw_tag:
                part += kw_tag
            blocks.append(part)
            total_shown += 1

    blocks.append(f'<font color="comment">ProBigA 每周前瞻 · {today_str} 生成</font>')

    ok = push_wecom_batched(blocks, header=header, webhook_url=WX_BRIEFING_URL)
    if ok:
        log.info("每周前瞻推送成功: %d 条相关信息", len(weekly_relevant))
        return len(weekly_relevant)
    else:
        log.error("每周前瞻推送失败")
        return 0


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="多源快讯同步 + 企业微信推送")
    parser.add_argument("--pages", type=int, default=2, help="每个源拉取页数")
    parser.add_argument("--no-push", action="store_true", help="只同步不推送")
    parser.add_argument("--push-only", action="store_true", help="只推送不同步")
    parser.add_argument("--mode", choices=["realtime", "daily", "weekly"], default="realtime",
                        help="推送模式: realtime=实时快讯, daily=每日早报, weekly=每周前瞻")
    args = parser.parse_args()

    engine = get_engine()

    if not args.push_only:
        log.info("开始同步多源快讯 (pages=%d)...", args.pages)
        with httpx.Client(headers={"User-Agent": "Mozilla/5.0 ProBigA-sync"}, timeout=15) as client:
            all_items = []
            try:
                cls_items = fetch_cls(client, args.pages)
                all_items.extend(cls_items)
                log.info("  财联社: %d 条", len(cls_items))
            except Exception as e:
                log.warning("  财联社失败: %s", e)
            try:
                em_items = fetch_eastmoney(client, max(1, args.pages // 2))
                all_items.extend(em_items)
                log.info("  东方财富: %d 条", len(em_items))
            except Exception as e:
                log.warning("  东方财富失败: %s", e)
            try:
                sina_items = fetch_sina(client, max(1, args.pages // 2))
                all_items.extend(sina_items)
                log.info("  新浪财经: %d 条", len(sina_items))
            except Exception as e:
                log.warning("  新浪财经失败: %s", e)

        if all_items:
            saved = save_to_db(engine, all_items)
            log.info("同步完成: %d 条入库 (总计拉取 %d 条)", saved, len(all_items))
        else:
            log.warning("所有源均无数据")

    if not args.no_push:
        if args.mode == "daily":
            log.info("推送每日早报...")
            pushed = push_daily_briefing(engine)
        elif args.mode == "weekly":
            if datetime.now().weekday() != 6:
                log.info("今天不是周日 (weekday=%d)，跳过每周前瞻", datetime.now().weekday())
                pushed = 0
            else:
                log.info("推送每周前瞻...")
                pushed = push_weekly_preview(engine)
        else:
            log.info("检查重要快讯并推送...")
            pushed = push_important(engine)

        if pushed:
            log.info("推送完成: %d 条", pushed)
        else:
            log.info("无需推送")


if __name__ == "__main__":
    main()
