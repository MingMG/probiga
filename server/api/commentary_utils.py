from __future__ import annotations

import re
from datetime import date
from typing import Any

DATE_TOKEN_RE = re.compile(r"(?<!\d)(\d{1,2})月(\d{1,2})[日号](?!\d)")
ENTRY_RE = re.compile(r"^\s*(\d+)\s+(\d{6})\s+([^\s]+)\s*(.*)$")

SECTION_HINTS = ("方向", "板块", "赛道")
SKIP_PREFIXES = ("序号", "代码", "名称", "缠论结构分析")
LOGIC_TAGS = {
    "目标价": "券商目标价",
    "上调目标价": "券商上调",
    "资本开支": "资本开支",
    "国产替代": "国产替代",
    "订单": "订单逻辑",
    "储能": "储能逻辑",
    "算力": "算力逻辑",
    "AI": "AI逻辑",
    "景气度": "景气改善",
    "上市": "事件催化",
}


def _clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", (line or "").replace("\t", " ")).strip()


def _infer_full_date(month: int, day: int, *, default_year: int) -> str:
    return f"{default_year:04d}-{month:02d}-{day:02d}"


def extract_dates(text: str, *, default_year: int) -> list[str]:
    dates: list[str] = []
    seen: set[str] = set()
    for month_s, day_s in DATE_TOKEN_RE.findall(text or ""):
        full = _infer_full_date(int(month_s), int(day_s), default_year=default_year)
        if full not in seen:
            seen.add(full)
            dates.append(full)
    return dates


def extract_logic_tags(text: str) -> list[str]:
    found: list[str] = []
    lowered = text or ""
    for keyword, label in LOGIC_TAGS.items():
        if keyword in lowered and label not in found:
            found.append(label)
    return found


def parse_commentary_text(
    text: str,
    *,
    reference_date: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    today = today or date.today()
    default_year = int((reference_date or today.isoformat())[:4])
    parsed_reference_date = reference_date
    if not parsed_reference_date:
        dates = extract_dates(text, default_year=default_year)
        parsed_reference_date = dates[0] if dates else today.isoformat()

    items: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    current_sector = ""

    for raw_line in (text or "").splitlines():
        line = _clean_line(raw_line)
        if not line:
            continue
        if any(prefix in line for prefix in SKIP_PREFIXES):
            continue
        if any(hint in line for hint in SECTION_HINTS) and not ENTRY_RE.match(line):
            current_sector = line
            continue

        match = ENTRY_RE.match(line)
        if match:
            if current:
                current["description"] = current["description"].strip()
                current["anchor_dates"] = extract_dates(current["description"], default_year=default_year)
                current["logic_tags"] = extract_logic_tags(current["description"])
                items.append(current)
            current = {
                "index": int(match.group(1)),
                "stock_code": match.group(2),
                "stock_name": match.group(3),
                "sector": current_sector,
                "description": match.group(4).strip(),
            }
            continue

        if current:
            current["description"] = (current["description"] + " " + line).strip()

    if current:
        current["description"] = current["description"].strip()
        current["anchor_dates"] = extract_dates(current["description"], default_year=default_year)
        current["logic_tags"] = extract_logic_tags(current["description"])
        items.append(current)

    return {
        "reference_date": parsed_reference_date,
        "items": items,
    }


def build_rule_checks(
    *,
    phase: str,
    current_price: float | None,
    ma5: float | None,
    ma10: float | None,
    support: float | None,
    anchor_low: float | None,
    anchor_volume: float | None,
    latest_volume: float | None,
    news_count: int,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    if current_price is None:
        checks.append({"key": "current_price", "status": "fail", "label": "实时价格", "detail": "当前未取到价格"})
        return checks

    if anchor_low:
        hold_anchor = current_price >= anchor_low * 0.995
        checks.append({
            "key": "anchor_low_hold",
            "status": "pass" if hold_anchor else "fail",
            "label": "启动低点是否失守",
            "detail": f"现价 {current_price:.2f} / 启动低点 {anchor_low:.2f}",
        })

    if ma5 and ma10:
        if current_price >= ma5:
            status = "pass"
        elif current_price >= ma10:
            status = "warn"
        else:
            status = "fail"
        checks.append({
            "key": "ma_position",
            "status": status,
            "label": "均线位置",
            "detail": f"现价 {current_price:.2f} / MA5 {ma5:.2f} / MA10 {ma10:.2f}",
        })

    if support:
        support_ok = current_price >= support * 0.995
        checks.append({
            "key": "support_hold",
            "status": "pass" if support_ok else "warn",
            "label": "短线支撑",
            "detail": f"现价 {current_price:.2f} / 支撑 {support:.2f}",
        })

    if anchor_volume and latest_volume:
        ratio = latest_volume / anchor_volume if anchor_volume else None
        if ratio is None:
            status = "warn"
        elif ratio <= 0.80:
            status = "pass"
        elif ratio <= 1.05:
            status = "warn"
        else:
            status = "fail"
        checks.append({
            "key": "pullback_volume",
            "status": status,
            "label": "回调量能",
            "detail": f"最新量 / 启动量 = {ratio:.2f}x" if ratio is not None else "量能不足以判断",
        })

    checks.append({
        "key": "news_support",
        "status": "pass" if news_count > 0 else "warn",
        "label": "消息面",
        "detail": f"近端匹配到 {news_count} 条相关新闻",
    })

    checks.append({
        "key": "phase_note",
        "status": "info",
        "label": "判定模式",
        "detail": "盘中更看现价与量价确认" if phase == "intraday" else "盘前更看日线位置与消息催化",
    })
    return checks


def build_verdict(checks: list[dict[str, Any]]) -> dict[str, str]:
    has_fail = any(c.get("status") == "fail" for c in checks if c.get("key") != "current_price")
    pass_count = sum(1 for c in checks if c.get("status") == "pass")
    warn_count = sum(1 for c in checks if c.get("status") == "warn")

    if any(c.get("key") == "current_price" and c.get("status") == "fail" for c in checks):
        return {"status": "NO_DATA", "summary": "当前没有拿到可用价格，先补行情数据。"}
    if has_fail:
        return {"status": "RISK", "summary": "至少一条关键规则失效，这条旧股评不宜直接沿用。"}
    if pass_count >= 3 and warn_count <= 1:
        return {"status": "TRACK", "summary": "规则近似上仍在跟踪区间，可继续观察盘前或盘中确认。"}
    return {"status": "WATCH", "summary": "结构没有明确破坏，但还缺进一步确认，适合放入观察池。"}


def project_feasibility_summary(minute_source: dict[str, Any] | None = None) -> dict[str, Any]:
    source = minute_source or {}
    return {
        "overall": "high",
        "premarket": "high",
        "intraday": "medium",
        "premarket_reason": "项目现有日线、资金流、快讯和个股详情接口，足够支撑盘前验证。",
        "intraday_reason": "项目已有实时行情和分钟线入口，但缠论二买仍只能做规则近似，不能当成确定信号。",
        "gaps": [
            "缠论二买属于主观结构判断，当前只能规则化近似。",
            "盘中可靠性依赖实时行情与分钟线覆盖情况。",
            "消息催化能匹配新闻和公告，但无法完全替代人工语义判断。",
        ],
        "minute_source": source,
    }
