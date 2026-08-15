# -*- coding: utf-8 -*-
"""Theme-first forecast for the immutable 09:08 premarket decision window.

The legacy recommendation batch is stock-first: sector rotation and external
markets only make small adjustments after a stock has already passed the base
screen.  This module deliberately reverses that order:

1. use only information available at the 09:08 cutoff;
2. rank dynamic A-share themes from catalysts, prior-session breadth/flow,
   overseas resonance and technical readiness;
3. select stocks from the database-backed constituents of the winning themes;
4. persist the result under a dedicated stage so later auction/intraday runs
   cannot overwrite what was actually known before the open.

Canonical families are semantic buckets, not fixed stock pools.  Every stock
membership comes from the current concept/industry tables and emerging labels
that do not match a family remain independently rankable.
"""
from __future__ import annotations

import json
import logging
import math
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from biz.market_context.external_market import (
    _score_snapshot,
    load_latest_external_market_context,
)
from server.common.sql_reader import read_sql_rows

logger = logging.getLogger(__name__)

MODEL_VERSION = "PREMARKET_THEME_V1.3_FLOW_CALIBRATED"
PREMARKET_STAGE = "PREMARKET_0908"
DEFAULT_THEME_LIMIT = 12
DEFAULT_STOCKS_PER_THEME = 5


@dataclass(frozen=True)
class ThemeFamily:
    key: str
    name: str
    keywords: tuple[str, ...]
    overseas_weights: tuple[tuple[str, float], ...]
    commodity_symbols: tuple[str, ...] = ()
    preferred_regions: tuple[str, ...] = ()


THEME_FAMILIES: tuple[ThemeFamily, ...] = (
    ThemeFamily(
        "battery_lithium",
        "锂电、电池与储能产业链",
        (
            "锂", "电池", "固态", "电解液", "隔膜", "正极", "负极", "盐湖",
            "钴", "镍", "储能", "宁德时代", "新能源车", "动力电池", "充电桩",
        ),
        (("kr_battery", 0.22), ("jp_battery", 0.15), ("us_lithium", 0.22),
         ("kospi", 0.08), ("nikkei", 0.08), ("nasdaq", 0.08),
         ("copper", 0.17)),
        ("copper", "crude_oil"),
        ("korea", "japan", "us"),
    ),
    ThemeFamily(
        "semiconductor",
        "半导体、芯片与电子元件",
        (
            "半导体", "芯片", "存储芯片", "存储器", "封测", "光刻", "晶圆", "硅片", "先进封装",
            "电子", "消费电子", "电子元件", "MLCC", "PCB", "被动元件", "集成电路", "第三代半导体",
        ),
        (("us_semiconductor", 0.25), ("taiwan_semiconductor", 0.20),
         ("kr_semiconductor", 0.20), ("jp_semiconductor", 0.20),
         ("nasdaq", 0.15)),
        ("copper",),
        ("us", "korea", "japan", "taiwan"),
    ),
    ThemeFamily(
        "ai_compute",
        "AI、算力与通信产业链",
        (
            "人工智能", "AI", "AIGC", "AIPC", "算力", "数据中心", "AIDC", "CPO", "光模块", "液冷",
            "服务器", "云计算", "大数据", "通信", "通信设备", "光通信", "数据要素", "边缘计算",
        ),
        (("us_ai", 0.24), ("us_semiconductor", 0.18),
         ("taiwan_semiconductor", 0.16), ("kr_semiconductor", 0.14),
         ("jp_semiconductor", 0.10), ("nasdaq", 0.18)),
        ("copper",),
        ("us", "korea", "japan", "taiwan"),
    ),
    ThemeFamily(
        "robotics",
        "机器人与高端自动化",
        ("机器人", "人形", "减速器", "伺服", "机器视觉", "工业母机", "自动化", "传感器"),
        (("jp_robotics", 0.30), ("us_robotics", 0.25), ("nikkei", 0.15),
         ("kospi", 0.10), ("nasdaq", 0.12), ("taiwan", 0.08)),
        ("copper",),
        ("japan", "korea", "us"),
    ),
    ThemeFamily(
        "power_grid",
        "电力、电网与能源设备",
        ("电力", "电网", "特高压", "电气设备", "电源设备", "光伏", "风电", "核电", "智能电网", "生物质能"),
        (("us_clean_energy", 0.28), ("sp500", 0.14), ("nikkei", 0.12),
         ("kospi", 0.10), ("copper", 0.24), ("crude_oil", 0.12)),
        ("copper", "crude_oil"),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "biomedicine",
        "创新药、医疗与生物科技",
        ("创新药", "医药", "医疗", "CRO", "生物", "疫苗", "中药", "化学制药", "医疗器械", "基因芯片", "生物芯片"),
        (("us_biotech", 0.42), ("nasdaq", 0.20), ("sp500", 0.18),
         ("nikkei", 0.10), ("kospi", 0.10)),
        (),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "resources",
        "资源品与周期涨价链",
        ("有色", "黄金", "白银", "铜", "铝", "稀土", "煤炭", "油气", "化工", "贵金属", "资源"),
        (("copper", 0.35), ("gold", 0.25), ("silver", 0.15),
         ("crude_oil", 0.15), ("sp500", 0.10)),
        ("copper", "gold", "silver", "crude_oil"),
        ("us",),
    ),
    ThemeFamily(
        "auto_chain",
        "汽车、智能驾驶与零部件",
        ("汽车", "无人驾驶", "智能驾驶", "车联网", "汽车电子", "零部件", "一体化压铸"),
        (("jp_auto", 0.25), ("us_auto", 0.20), ("jp_battery", 0.12),
         ("nikkei", 0.13), ("kospi", 0.10), ("nasdaq", 0.08),
         ("copper", 0.12)),
        ("copper", "crude_oil"),
        ("japan", "korea", "us"),
    ),
    ThemeFamily(
        "defense_aerospace",
        "国防军工与商业航天",
        ("军工", "国防", "航天", "航空", "卫星", "低空经济", "无人机", "商业航天"),
        (("us_defense", 0.35), ("sp500", 0.18), ("nasdaq", 0.12),
         ("nikkei", 0.15), ("kospi", 0.08), ("gold", 0.12)),
        ("gold", "crude_oil"),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "software_security",
        "国产软件、金融科技与网络安全",
        ("软件", "网络安全", "信创", "操作系统", "数据库", "金融科技", "互联网金融", "数字货币", "支付"),
        (("us_software", 0.28), ("us_cybersecurity", 0.18),
         ("nasdaq", 0.24), ("sp500", 0.12), ("nikkei", 0.08),
         ("kospi", 0.05), ("taiwan", 0.05)),
        (),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "consumer",
        "消费、传媒与服务业",
        ("消费", "白酒", "食品", "家电", "旅游", "酒店", "传媒", "影视", "游戏", "零售"),
        (("us_consumer", 0.30), ("sp500", 0.18), ("hang_seng", 0.20),
         ("nikkei", 0.14), ("kospi", 0.10), ("nasdaq", 0.08)),
        (),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "finance_property",
        "金融与地产链",
        ("证券", "银行", "保险", "房地产", "地产", "多元金融"),
        (("us_financial", 0.30), ("sp500", 0.18), ("hang_seng", 0.24),
         ("nikkei", 0.14), ("kospi", 0.14)),
        (),
        ("us", "japan", "korea"),
    ),
    ThemeFamily(
        "agriculture",
        "农业、养殖与食品供给",
        ("农业", "种业", "猪肉", "养殖", "粮食", "农产品", "饲料"),
        (("us_agriculture", 0.35), ("sp500", 0.20), ("crude_oil", 0.20),
         ("usdcnh", -0.15), ("gold", 0.10)),
        ("crude_oil", "gold"),
        ("us",),
    ),
)

_FAMILY_BY_KEY = {item.key: item for item in THEME_FAMILIES}

_EXCLUDED_LABELS = {
    "融资融券", "转融券标的", "深股通", "沪股通", "富时罗素概念", "富时罗素概念股",
    "标普道琼斯A股", "证金汇金", "预盈预增", "增持回购", "中盘", "小盘", "大盘",
    "基金重仓", "机构重仓", "社保重仓", "昨日涨停", "昨日连板", "高送转", "ST股",
}
_EXCLUDED_LABEL_TOKENS = ("融资融券", "转融券", "深股通", "沪股通", "预盈预增", "昨日")

_TRIGGER_PATTERNS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    ("SUPPLY_DISRUPTION", ("停产", "断供", "供应中断", "制裁", "禁运", "短缺", "库存告急"), 18.0),
    ("PRICE_INCREASE", ("涨价", "提价", "价格上调", "报价上调", "价格上涨"), 13.0),
    ("ORDER", ("订单", "中标", "签约", "合同", "采购"), 12.0),
    ("POLICY", ("政策", "规划", "补贴", "指导意见", "纳入", "获批"), 11.0),
    ("CAPACITY", ("扩产", "投产", "量产", "产能", "开工"), 9.0),
    ("TECH_PRODUCT", ("发布", "突破", "验证", "测试", "首发", "迭代", "工程化"), 11.0),
    ("EARNINGS", ("预增", "扭亏", "超预期", "创历史新高", "业绩增长"), 10.0),
    ("DEMAND", ("需求增长", "销量增长", "渗透率提升", "排产提升", "景气回升"), 10.0),
)
_NEGATIVE_PATTERNS = ("需求不及预期", "下调指引", "订单取消", "价格下跌", "产能过剩", "减持", "调查", "立案")
_REGION_PATTERNS: Mapping[str, tuple[str, ...]] = {
    "us": ("美国", "美股", "纳斯达克", "标普", "英伟达", "特斯拉", "微软", "苹果", "谷歌", "亚马逊"),
    "korea": ("韩国", "韩股", "KOSPI", "三星", "SK海力士", "LG新能源", "SK On"),
    "japan": ("日本", "日股", "日经", "东京电子", "信越", "丰田", "本田", "松下", "村田"),
    "taiwan": ("台湾", "台股", "台积电", "联发科", "鸿海"),
}


def _number(value: Any, default: float | None = None) -> float | None:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, str):
        value = value.strip().replace(",", "").replace("%", "")
        if not value or value.lower() in {"none", "null", "nan", "-", "--"}:
            return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text_value = str(value or "").strip()
    if not text_value:
        return None
    try:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text_value[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _date_text(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.date().isoformat() if parsed else str(value or "")[:10]


def _safe_mean(values: Iterable[Any], default: float = 50.0) -> float:
    numbers = [value for item in values if (value := _number(item)) is not None]
    return float(mean(numbers)) if numbers else float(default)


def _weighted_average(parts: Sequence[tuple[float | None, float]], default: float = 50.0) -> float:
    usable = [(float(value), float(weight)) for value, weight in parts if value is not None and weight > 0]
    if not usable:
        return float(default)
    weight_sum = sum(weight for _, weight in usable)
    return sum(value * weight for value, weight in usable) / weight_sum


def _normalize_label(value: Any) -> str:
    text_value = str(value or "").strip()
    text_value = re.sub(r"[\s·•_/（）()\-]+", "", text_value)
    return text_value.casefold()


def _meaningful_label(value: Any) -> bool:
    label = str(value or "").strip()
    if len(_normalize_label(label)) < 2:
        return False
    if label in _EXCLUDED_LABELS:
        return False
    return not any(token in label for token in _EXCLUDED_LABEL_TOKENS)


def _keyword_matches(keyword: str, text_value: Any) -> bool:
    """Match short ASCII abbreviations without accidental inner-word hits."""

    rendered = str(text_value or "")
    if not rendered:
        return False
    # A few two-character industry words are embedded in a different,
    # well-known industry phrase.  Treating those substrings as independent
    # themes made chip news look like a consumer catalyst and biomass power
    # look like medicine.
    if keyword == "消费":
        rendered = rendered.replace("消费电子", "").replace("消费级电子", "")
    elif keyword == "生物":
        rendered = rendered.replace("生物质能", "").replace("生物质发电", "")
    elif keyword == "电子":
        return _normalize_label(rendered) in {"电子", "电子行业", "电子制造", "电子器件"}
    if not rendered:
        return False
    if re.fullmatch(r"[A-Za-z0-9+.-]+", keyword):
        return bool(re.search(
            rf"(?<![A-Za-z0-9]){re.escape(keyword)}(?![A-Za-z0-9])",
            rendered,
            flags=re.IGNORECASE,
        ))
    return _normalize_label(keyword) in _normalize_label(rendered)


def family_for_label(label: Any) -> ThemeFamily | None:
    normalized = _normalize_label(label)
    if not normalized:
        return None
    best: tuple[int, ThemeFamily] | None = None
    for family in THEME_FAMILIES:
        matched = [keyword for keyword in family.keywords if _keyword_matches(keyword, label)]
        if not matched:
            continue
        longest = max(len(_normalize_label(keyword)) for keyword in matched)
        candidate = (longest, family)
        if best is None or candidate[0] > best[0]:
            best = candidate
    return best[1] if best else None


def classify_catalyst(title: Any, content: Any = "") -> dict[str, Any]:
    """Classify a catalyst only when the subject and event both match.

    A generic sentence containing ``涨价`` is not a resources catalyst.  The
    family must also be present (for example copper, lithium or chemicals),
    which prevents unrelated phone/software price stories polluting the theme.
    """

    text_value = f"{title or ''} {content or ''}".strip()
    normalized = _normalize_label(text_value)
    family_keys = []
    for family in THEME_FAMILIES:
        if any(_keyword_matches(keyword, text_value) for keyword in family.keywords):
            family_keys.append(family.key)
    triggers = []
    trigger_weight = 0.0
    for trigger_name, patterns, weight in _TRIGGER_PATTERNS:
        if any(pattern.casefold() in text_value.casefold() for pattern in patterns):
            triggers.append(trigger_name)
            trigger_weight += weight
    negative = any(pattern in text_value for pattern in _NEGATIVE_PATTERNS)
    regions = [
        region
        for region, patterns in _REGION_PATTERNS.items()
        if any(pattern.casefold() in text_value.casefold() for pattern in patterns)
    ]
    # Subject-only mentions are useful evidence but not positive catalysts.
    qualified = bool(family_keys and triggers)
    return {
        "family_keys": family_keys,
        "trigger_types": triggers,
        "trigger_weight": trigger_weight if qualified else 0.0,
        "regions": regions,
        "negative": negative,
        "qualified": qualified,
    }


def _theme_key(label: Any) -> tuple[str, str, str]:
    family = family_for_label(label)
    if family:
        return f"family:{family.key}", family.key, family.name
    normalized = _normalize_label(label)
    return f"concept:{normalized}", "dynamic", str(label or "").strip()


def score_external_theme(
    family_key: str,
    items: Sequence[Mapping[str, Any]],
    *,
    catalyst_regions: Sequence[str] = (),
) -> tuple[float, list[str]]:
    family = _FAMILY_BY_KEY.get(family_key)
    if family is None:
        score, _status, reason = _score_snapshot([dict(item) for item in items])
        return float(score if score is not None else 50.0), [reason] if reason else []
    by_symbol = {
        str(item.get("symbol") or ""): item
        for item in items
        if str(item.get("availability") or "available") == "available"
    }
    parts: list[tuple[float | None, float]] = []
    evidence: list[str] = []
    for symbol, weight in family.overseas_weights:
        change = _number(by_symbol.get(symbol, {}).get("change_pct"))
        if change is None:
            continue
        parts.append((change if weight >= 0 else -change, abs(weight)))
        name = str(by_symbol[symbol].get("display_name") or symbol)
        evidence.append(f"{name}{change:+.2f}%")
    weighted_change = _weighted_average(parts, default=0.0)
    score = 50.0 + weighted_change * 7.0
    region_hits = sorted(set(catalyst_regions) & set(family.preferred_regions))
    if region_hits:
        score += min(8.0, len(region_hits) * 3.0)
        labels = {"us": "美国", "korea": "韩国", "japan": "日本", "taiwan": "中国台湾"}
        evidence.append("海外产业催化：" + "、".join(labels.get(item, item) for item in region_hits))
    return round(_clamp(score), 1), evidence[:6]


def _commodity_score(family_key: str, items: Sequence[Mapping[str, Any]], catalyst: float) -> tuple[float, list[str]]:
    family = _FAMILY_BY_KEY.get(family_key)
    if family is None or not family.commodity_symbols:
        return round(_clamp(45.0 + max(0.0, catalyst - 50.0) * 0.15), 1), []
    by_symbol = {str(item.get("symbol") or ""): item for item in items}
    changes = []
    evidence = []
    for symbol in family.commodity_symbols:
        change = _number(by_symbol.get(symbol, {}).get("change_pct"))
        if change is None:
            continue
        changes.append(change)
        evidence.append(f"{by_symbol[symbol].get('display_name') or symbol}{change:+.2f}%")
    if not changes:
        return 45.0, []
    raw = _safe_mean(changes, default=0.0)
    # For battery/auto/power chains copper is a demand proxy. Crude oil is not
    # allowed to dominate the score; the crowding/risk layer handles shocks.
    return round(_clamp(50.0 + raw * 6.0), 1), evidence[:4]


def _news_for_cutoff(news_rows: Sequence[Mapping[str, Any]], cutoff_at: datetime) -> list[dict[str, Any]]:
    selected = []
    lower_bound = cutoff_at - timedelta(hours=60)
    for row in news_rows:
        publish_time = _parse_datetime(row.get("publish_time"))
        if publish_time is None or publish_time > cutoff_at or publish_time < lower_bound:
            continue
        classified = classify_catalyst(row.get("title"), row.get("content"))
        selected.append({**dict(row), "publish_time": publish_time, "classification": classified})
    return selected


def _catalyst_score(
    family_key: str,
    theme_name: str,
    news_rows: Sequence[Mapping[str, Any]],
    cutoff_at: datetime,
) -> tuple[float, list[str], list[str]]:
    positive = 0.0
    negative = 0.0
    evidence: list[str] = []
    regions: list[str] = []
    normalized_name = _normalize_label(theme_name)
    for row in news_rows:
        classification = row["classification"]
        text_value = f"{row.get('title') or ''} {row.get('content') or ''}"
        exact_dynamic_match = family_key == "dynamic" and normalized_name in _normalize_label(text_value)
        family_match = family_key in classification["family_keys"]
        if not family_match and not exact_dynamic_match:
            continue
        age_hours = max(0.0, (cutoff_at - row["publish_time"]).total_seconds() / 3600.0)
        freshness = 1.0 if age_hours <= 6 else (0.85 if age_hours <= 18 else (0.65 if age_hours <= 36 else 0.45))
        weight = float(classification.get("trigger_weight") or 0.0)
        if classification.get("negative"):
            negative += max(8.0, weight) * freshness
        elif classification.get("qualified") or exact_dynamic_match:
            positive += max(6.0, weight) * freshness
        else:
            continue
        regions.extend(classification.get("regions") or [])
        headline = str(row.get("title") or "").strip()
        if not headline:
            headline = re.sub(r"^【[^】]+】", "", str(row.get("content") or "").strip())
        if headline:
            evidence.append(f"{headline[:46]}（{row['publish_time'].strftime('%m-%d %H:%M')}）")
    score = 35.0 + min(65.0, positive) - min(45.0, negative)
    return round(_clamp(score), 1), evidence[:5], regions


def _row_theme_key(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    label = row.get("theme_name") or row.get("concept_name") or row.get("index_name") or row.get("plate_name") or row.get("name")
    if not _meaningful_label(label):
        return None
    return _theme_key(label)


def _hot_row_score(row: Mapping[str, Any]) -> float:
    rank = max(1.0, _number(row.get("rank"), 50.0) or 50.0)
    change = _number(row.get("change_pct"), 0.0) or 0.0
    return _clamp(96.0 - (rank - 1.0) * 2.8 + max(-4.0, min(4.0, change)) * 2.0)


def _flow_row_score(row: Mapping[str, Any]) -> float:
    rate = _number(row.get("main_net_inflow_rate"))
    amount = _number(row.get("main_net_inflow"))
    change = _number(row.get("change_pct"), 0.0) or 0.0
    if rate is not None:
        return _clamp(50.0 + rate * 3.0 + change * 1.5)
    if amount is not None:
        sign_score = 62.0 if amount > 0 else 38.0
        return _clamp(sign_score + change * 1.5)
    return _clamp(50.0 + change * 2.0)


def _flow_history_by_code(
    rows: Sequence[Mapping[str, Any]],
    *,
    source_trade_date: str,
    limit: int = 5,
) -> dict[str, list[dict[str, Any]]]:
    """Return point-in-time stock-flow histories, newest session first."""

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_date = str(source_trade_date or "")[:10]
    for row in rows:
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if not re.fullmatch(r"[036]\d{5}", code):
            continue
        trade_date = _date_text(row.get("trade_date")) if row.get("trade_date") else ""
        if trade_date and source_date and trade_date > source_date:
            continue
        grouped[code].append(dict(row))
    for code, history in grouped.items():
        history.sort(
            key=lambda row: (
                bool(row.get("trade_date")),
                _date_text(row.get("trade_date")) if row.get("trade_date") else "",
            ),
            reverse=True,
        )
        grouped[code] = history[: max(1, int(limit))]
    return dict(grouped)


def _flow_direction(value: Any, turnover_amount: Any = None) -> tuple[float | None, float | None]:
    """Map a cash flow to [-1, 1], normalized by turnover when available."""

    cash_flow = _number(value)
    if cash_flow is None:
        return None, None
    turnover = _number(turnover_amount)
    if turnover is not None and turnover > 0:
        rate = cash_flow / turnover * 100.0
        return math.tanh(rate / 4.0), rate
    if cash_flow > 0:
        return 0.35, None
    if cash_flow < 0:
        return -0.35, None
    return 0.0, None


def _flow_resonance_score(main_direction: float | None, theme_flow_score: float | None) -> float | None:
    if theme_flow_score is None or main_direction is None:
        return None
    theme_direction = max(-1.0, min(1.0, (float(theme_flow_score) - 50.0) / 35.0))
    aligned = main_direction * theme_direction
    return _clamp(
        50.0
        + 22.0 * main_direction
        + 18.0 * theme_direction
        + 12.0 * max(0.0, aligned)
        - 12.0 * max(0.0, -aligned)
    )


def _stock_flow_factor_details(
    flow_history: Sequence[Mapping[str, Any]],
    kline: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one de-duplicated, turnover-normalized five-session flow profile."""

    history = [dict(row) for row in flow_history[:5]]
    if not history:
        return {"score": None, "quality_status": "BLOCK", "history_days": 0}
    recency_weights = (5.0, 4.0, 3.0, 2.0, 1.0)
    main_parts: list[tuple[float, float]] = []
    order_parts: list[tuple[float, float]] = []
    main_rate_parts: list[tuple[float, float]] = []
    positive_days = 0
    normalized_days = 0
    latest_direction: float | None = None
    latest_rate: float | None = None

    for index, row in enumerate(history):
        turnover = _number(row.get("turnover_amount"))
        if (turnover is None or turnover <= 0) and index == 0:
            turnover = _number(kline.get("amount"))
        main_direction, main_rate = _flow_direction(row.get("main_net_inflow"), turnover)
        if index == 0:
            latest_direction = main_direction
            latest_rate = main_rate
        if main_direction is not None:
            main_parts.append((main_direction, recency_weights[index]))
            positive_days += int((_number(row.get("main_net_inflow"), 0.0) or 0.0) > 0)
            if main_rate is not None:
                main_rate_parts.append((main_rate, recency_weights[index]))
                normalized_days += 1

        max_direction, _ = _flow_direction(row.get("max_net_inflow"), turnover)
        large_direction, _ = _flow_direction(row.get("lg_net_inflow"), turnover)
        day_order = [value for value in (max_direction, large_direction) if value is not None]
        if day_order:
            order_parts.append((_safe_mean(day_order, default=0.0), recency_weights[index]))

    if latest_direction is None and not main_parts:
        return {"score": None, "quality_status": "BLOCK", "history_days": len(history)}
    intensity_score = _clamp(50.0 + 45.0 * (latest_direction or 0.0))
    continuity_direction = _weighted_average(main_parts, default=0.0)
    continuity_score = _clamp(50.0 + 40.0 * continuity_direction)
    order_score = (
        _clamp(50.0 + 35.0 * _weighted_average(order_parts, default=0.0))
        if order_parts else None
    )
    base_score = _weighted_average(
        ((intensity_score, 0.20), (continuity_score, 0.50), (order_score, 0.30)),
        default=50.0,
    )
    quality_status = "PASS" if normalized_days >= 3 and len(main_parts) >= 3 else (
        "WATCH" if latest_direction is not None else "BLOCK"
    )
    return {
        "score": round(_clamp(base_score), 1),
        "raw_score": round(_clamp(base_score), 1),
        "intensity_score": round(intensity_score, 1),
        "continuity_score": round(continuity_score, 1),
        "order_score": round(order_score, 1) if order_score is not None else None,
        "latest_direction": latest_direction,
        "latest_rate": round(latest_rate, 4) if latest_rate is not None else None,
        "five_day_rate": round(
            _weighted_average(main_rate_parts, default=0.0), 4
        ) if main_rate_parts else None,
        "history_days": len(main_parts),
        "normalized_days": normalized_days,
        "positive_days": positive_days,
        "quality_status": quality_status,
    }


def _percentile_scores(values: Mapping[str, float]) -> dict[str, float]:
    """Return mid-rank percentile scores while keeping ties identical."""

    if not values:
        return {}
    ordered = sorted((float(value), code) for code, value in values.items())
    if len(ordered) == 1:
        return {ordered[0][1]: 50.0}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][0] == ordered[index][0]:
            end += 1
        mid_rank = (index + end - 1) / 2.0
        score = 100.0 * mid_rank / (len(ordered) - 1)
        for position in range(index, end):
            result[ordered[position][1]] = round(score, 1)
        index = end
    return result


def _neutralize_flow_profiles(
    profiles: Mapping[str, Mapping[str, Any]],
    kline_by_code: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Neutralize individual flow strength within industry and market-cap peers."""

    output = {code: dict(profile) for code, profile in profiles.items()}
    industries: dict[str, dict[str, float]] = defaultdict(dict)
    caps: list[tuple[float, str]] = []
    for code, profile in output.items():
        raw_score = _number(profile.get("raw_score"))
        if raw_score is None:
            continue
        kline = kline_by_code.get(code, {})
        industry = str(kline.get("industry_name") or "").strip()
        if industry:
            industries[industry][code] = raw_score
        market_cap = _number(kline.get("market_cap"))
        if market_cap is not None and market_cap > 0:
            caps.append((market_cap, code))

    industry_percentiles: dict[str, float] = {}
    for members in industries.values():
        if len(members) >= 5:
            industry_percentiles.update(_percentile_scores(members))

    cap_buckets: dict[int, dict[str, float]] = defaultdict(dict)
    for rank, (_market_cap, code) in enumerate(sorted(caps)):
        bucket = min(4, int(rank * 5 / max(1, len(caps))))
        raw_score = _number(output[code].get("raw_score"))
        if raw_score is not None:
            cap_buckets[bucket][code] = raw_score
    cap_percentiles: dict[str, float] = {}
    for members in cap_buckets.values():
        if len(members) >= 5:
            cap_percentiles.update(_percentile_scores(members))

    for code, profile in output.items():
        raw_score = _number(profile.get("raw_score"))
        if raw_score is None:
            continue
        industry_score = industry_percentiles.get(code)
        cap_score = cap_percentiles.get(code)
        neutralized = _weighted_average(
            ((raw_score, 0.60), (industry_score, 0.25), (cap_score, 0.15)),
            default=raw_score,
        )
        profile["industry_percentile"] = industry_score
        profile["market_cap_percentile"] = cap_score
        profile["score"] = round(_clamp(neutralized), 1)
    return output


def _flow_profile_evidence(profile: Mapping[str, Any], resonance_score: float | None) -> list[str]:
    score = _number(profile.get("score"))
    evidence = [f"统一资金因子{score:.1f}"] if score is not None else []
    latest_rate = _number(profile.get("latest_rate"))
    if latest_rate is not None:
        evidence.append(f"主力净流入强度{latest_rate:+.2f}%")
    elif _number(profile.get("latest_direction")) is not None:
        evidence.append("主力净流入方向为正" if float(profile["latest_direction"]) > 0 else "主力净流入方向为负")
    history_days = int(profile.get("history_days") or 0)
    if history_days:
        evidence.append(f"近{history_days}日净流入{int(profile.get('positive_days') or 0)}日")
    if _number(profile.get("order_score")) is not None:
        evidence.append(f"近5日大单结构{float(profile['order_score']):.1f}")
        if float(profile["order_score"]) >= 55.0:
            evidence.append("大单与超大单方向一致")
    if _number(profile.get("industry_percentile")) is not None:
        evidence.append(f"行业资金分位{float(profile['industry_percentile']):.1f}")
    if _number(profile.get("market_cap_percentile")) is not None:
        evidence.append(f"同市值资金分位{float(profile['market_cap_percentile']):.1f}")
    if resonance_score is not None:
        evidence.append(f"板块个股资金共振{resonance_score:.1f}")
    return evidence


def _flow_dataset_quality(
    profiles: Mapping[str, Mapping[str, Any]],
    kline_by_code: Mapping[str, Mapping[str, Any]],
    flow_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Audit coverage, duplicate rows and impossible flow/turnover ratios."""

    expected = len(kline_by_code)
    covered = sum(_number(profile.get("score")) is not None for profile in profiles.values())
    normalized = sum(int(profile.get("normalized_days") or 0) >= 3 for profile in profiles.values())
    seen: set[tuple[str, str]] = set()
    duplicates = 0
    invalid = 0
    comparable = 0
    for row in flow_rows:
        code = str(row.get("stock_code") or "").zfill(6)
        trade_date = _date_text(row.get("trade_date")) if row.get("trade_date") else ""
        key = (code, trade_date)
        if trade_date and key in seen:
            duplicates += 1
        elif trade_date:
            seen.add(key)
        turnover = _number(row.get("turnover_amount"))
        main_flow = _number(row.get("main_net_inflow"))
        if turnover is not None and turnover > 0 and main_flow is not None:
            comparable += 1
            invalid += int(abs(main_flow / turnover) > 1.0)

    coverage = covered / expected if expected else 0.0
    normalized_coverage = normalized / expected if expected else 0.0
    invalid_ratio = invalid / comparable if comparable else 0.0
    if expected and coverage >= 0.80 and normalized_coverage >= 0.65 and not duplicates and invalid_ratio <= 0.01:
        status = "PASS"
    elif covered and coverage >= 0.25 and invalid_ratio <= 0.05:
        status = "WATCH"
    else:
        status = "BLOCK"
    return {
        "status": status,
        "expected_stocks": expected,
        "covered_stocks": covered,
        "coverage_ratio": round(coverage, 4),
        "normalized_stocks": normalized,
        "normalized_coverage_ratio": round(normalized_coverage, 4),
        "duplicate_rows": duplicates,
        "invalid_ratio_rows": invalid,
        "comparable_rows": comparable,
        "invalid_ratio": round(invalid_ratio, 4),
    }


def _stock_flow_factor_score(
    flow_history: Sequence[Mapping[str, Any]],
    kline: Mapping[str, Any],
    *,
    theme_flow_score: float | None,
) -> tuple[float | None, list[str]]:
    """Score intensity, persistence, order structure and theme resonance.

    Absolute cash amounts are never compared directly across stocks.  The latest
    flow is divided by daily turnover, while history is used only for direction
    and persistence.  This avoids a structural large-cap bias.
    """

    profile = _stock_flow_factor_details(flow_history, kline)
    base_score = _number(profile.get("score"))
    if base_score is None:
        return None, []
    resonance_score = _flow_resonance_score(
        _number(profile.get("latest_direction")),
        theme_flow_score,
    )
    score = _weighted_average(((base_score, 0.85), (resonance_score, 0.15)), default=base_score)
    rendered = dict(profile)
    rendered["score"] = round(_clamp(score), 1)
    return rendered["score"], _flow_profile_evidence(rendered, resonance_score)


def _latest_by_code(rows: Sequence[Mapping[str, Any]], code_field: str = "stock_code") -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get(code_field) or "").zfill(6)
        if code and code != "000000":
            result[code] = dict(row)
    return result


def _stock_candidate_score(
    code: str,
    theme_score: float,
    theme_match_count: int,
    theme_v3_score: float | None,
    alpha: Mapping[str, Any],
    analysis: Mapping[str, Any],
    recommendation: Mapping[str, Any],
    kline: Mapping[str, Any],
    flow: Mapping[str, Any],
    flow_history: Sequence[Mapping[str, Any]] = (),
    theme_flow_score: float | None = None,
    flow_profile: Mapping[str, Any] | None = None,
    flow_dataset_status: str = "WATCH",
) -> tuple[float, float, str, list[str]]:
    short_score = _number(analysis.get("short_term_score"), 50.0) or 50.0
    technical = _number(analysis.get("technical_score"), _number(recommendation.get("technical"), 50.0)) or 50.0
    base_capital = _number(analysis.get("capital_score"), _number(recommendation.get("capital_score"), 50.0)) or 50.0
    event_score = _number(analysis.get("event_score"), _number(recommendation.get("event_score"), 50.0)) or 50.0
    final_trade = _number(recommendation.get("final_trade_score"), _number(recommendation.get("ai_score"), 50.0)) or 50.0
    v3_alpha = _number(alpha.get("v3_alpha_score"), 50.0) or 50.0
    v3_theme = _number(theme_v3_score, 45.0) or 45.0
    change = _number(kline.get("change_pct"), 0.0) or 0.0
    setup_score = 68.0 if -2.5 <= change <= 3.5 else (55.0 if -5.0 <= change < -2.5 else 42.0)
    effective_history = list(flow_history) or ([dict(flow)] if flow else [])
    profile = dict(flow_profile or _stock_flow_factor_details(effective_history, kline))
    base_flow_factor = _number(profile.get("score"))
    resonance = _flow_resonance_score(_number(profile.get("latest_direction")), theme_flow_score)
    flow_factor = (
        _weighted_average(((base_flow_factor, 0.85), (resonance, 0.15)), default=base_flow_factor or 50.0)
        if base_flow_factor is not None else None
    )
    flow_evidence = _flow_profile_evidence({**profile, "score": flow_factor}, resonance) if flow_factor is not None else []
    unified_flow = flow_factor if flow_factor is not None else base_capital
    flow_fallback = flow_factor is None
    theme_match_score = _clamp(52.0 + min(5, max(1, int(theme_match_count))) * 8.0)
    raw = (
        theme_score * 0.20
        + theme_match_score * 0.08
        + v3_alpha * 0.12
        + v3_theme * 0.14
        + short_score * 0.11
        + technical * 0.11
        + unified_flow * 0.12
        + event_score * 0.05
        + final_trade * 0.03
        + setup_score * 0.04
    )
    penalty = max(0.0, (change - 5.0) * 2.0)
    if theme_match_count <= 1 and v3_theme < 50.0:
        penalty += 8.0
    elif theme_match_count <= 1 and v3_theme < 60.0:
        penalty += 4.0
    chase_status = str(recommendation.get("chase_risk_status") or analysis.get("chase_risk_status") or "").upper()
    if chase_status == "BLOCK":
        penalty += 22.0
    recommend_status = str(analysis.get("recommend_status") or recommendation.get("recommend_status") or "ALLOW").upper()
    if recommend_status == "BLOCK":
        penalty += 8.0
    elif recommend_status == "SUSPENDED":
        penalty += 3.0
    ordinary_eligible = recommendation.get("ordinary_buy_eligible", analysis.get("ordinary_buy_eligible"))
    risk_level = str(analysis.get("event_risk_level") or recommendation.get("event_risk_level") or "LOW").upper()
    if risk_level in {"HIGH", "CRITICAL"}:
        penalty += 18.0
    profile_quality = str(profile.get("quality_status") or "BLOCK").upper()
    dataset_quality = str(flow_dataset_status or "BLOCK").upper()
    if profile_quality == "WATCH":
        penalty += 2.0
    elif profile_quality == "BLOCK":
        penalty += 5.0
    if dataset_quality == "BLOCK":
        penalty += 8.0
    score = _clamp(raw - penalty)
    status = "盘前候选" if score >= 68.0 else ("重点观察" if score >= 60.0 else "暂缓")
    if dataset_quality == "BLOCK":
        status = "数据不足"
    evidence = [
        f"主题{theme_score:.1f}", f"主题命中{theme_match_count}项",
        f"V3个股{v3_alpha:.1f}", f"V3主题个股{v3_theme:.1f}",
        f"短线{short_score:.1f}", f"技术{technical:.1f}",
        f"统一资金{unified_flow:.1f}", f"昨涨跌{change:+.2f}%",
    ]
    evidence.extend(flow_evidence)
    if flow_fallback:
        evidence.append("资金流水不足，暂用历史资金评分且降级")
    evidence.append(f"资金数据质量{profile_quality}/{dataset_quality}")
    if penalty > 0:
        evidence.append(f"门禁/风险扣分{penalty:.1f}")
    if theme_match_count <= 1 and v3_theme < 60.0:
        evidence.append("主题关联度偏弱")
    if ordinary_eligible in {0, False, "0", "false", "False"}:
        evidence.append("开盘买入门禁仍需确认")
    return round(score, 1), round(penalty, 1), status, evidence


def build_theme_forecast_from_records(
    *,
    session_date: str,
    source_trade_date: str,
    cutoff_at: datetime | str,
    hot_rows: Sequence[Mapping[str, Any]] = (),
    concept_flow_rows: Sequence[Mapping[str, Any]] = (),
    concept_market_rows: Sequence[Mapping[str, Any]] = (),
    news_rows: Sequence[Mapping[str, Any]] = (),
    membership_rows: Sequence[Mapping[str, Any]] = (),
    theme_signal_rows: Sequence[Mapping[str, Any]] = (),
    alpha_rows: Sequence[Mapping[str, Any]] = (),
    analysis_rows: Sequence[Mapping[str, Any]] = (),
    recommendation_rows: Sequence[Mapping[str, Any]] = (),
    kline_rows: Sequence[Mapping[str, Any]] = (),
    stock_flow_rows: Sequence[Mapping[str, Any]] = (),
    external_items: Sequence[Mapping[str, Any]] = (),
    external_summary: Mapping[str, Any] | None = None,
    theme_limit: int = DEFAULT_THEME_LIMIT,
    stocks_per_theme: int = DEFAULT_STOCKS_PER_THEME,
) -> dict[str, Any]:
    cutoff = _parse_datetime(cutoff_at)
    if cutoff is None:
        raise ValueError("cutoff_at must be a valid datetime")
    if cutoff.date().isoformat() != str(session_date)[:10]:
        raise ValueError("09:08 cutoff and session_date must be the same date")
    if str(source_trade_date)[:10] >= str(session_date)[:10]:
        raise ValueError("source_trade_date must be earlier than the 09:08 session date")

    selected_news = _news_for_cutoff(news_rows, cutoff)
    themes: dict[str, dict[str, Any]] = {}

    def ensure_theme(label: Any) -> dict[str, Any] | None:
        if not _meaningful_label(label):
            return None
        key, family_key, display_name = _theme_key(label)
        item = themes.setdefault(key, {
            "theme_key": key,
            "family_key": family_key,
            "theme_name": display_name,
            "labels": set(),
            "members": set(),
            "members_by_label": defaultdict(set),
            "hot_rows": [],
            "flow_rows": [],
            "market_rows": [],
            "strategy_rows": [],
        })
        item["labels"].add(str(label).strip())
        return item

    for row in membership_rows:
        label = row.get("theme_name") or row.get("concept_name") or row.get("plate_name") or row.get("name")
        theme = ensure_theme(label)
        if theme is None:
            continue
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if re.fullmatch(r"[036]\d{5}", code):
            theme["members"].add(code)
            theme["members_by_label"][str(label).strip()].add(code)
    for collection, bucket in ((hot_rows, "hot_rows"), (concept_flow_rows, "flow_rows"), (concept_market_rows, "market_rows")):
        for row in collection:
            label = row.get("theme_name") or row.get("concept_name") or row.get("index_name") or row.get("plate_name") or row.get("name")
            theme = ensure_theme(label)
            if theme is not None:
                theme[bucket].append(dict(row))
    for row in theme_signal_rows:
        label = row.get("theme_name") or row.get("theme_code")
        theme = ensure_theme(label)
        if theme is None:
            continue
        theme["strategy_rows"].append(dict(row))
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if re.fullmatch(r"[036]\d{5}", code):
            theme["members"].add(code)
            theme["members_by_label"][str(label).strip()].add(code)
    for row in selected_news:
        for family_key in row["classification"].get("family_keys") or []:
            family = _FAMILY_BY_KEY[family_key]
            ensure_theme(family.name)

    analysis_by_code = _latest_by_code(analysis_rows)
    recommendation_by_code = _latest_by_code(recommendation_rows)
    alpha_by_code = _latest_by_code(alpha_rows)
    kline_by_code = _latest_by_code(kline_rows)
    stock_flow_history_by_code = _flow_history_by_code(
        stock_flow_rows,
        source_trade_date=source_trade_date,
        limit=5,
    )
    stock_flow_by_code = {
        code: history[0]
        for code, history in stock_flow_history_by_code.items()
        if history
    }
    raw_flow_profiles = {
        code: _stock_flow_factor_details(stock_flow_history_by_code.get(code, ()), kline)
        for code, kline in kline_by_code.items()
    }
    stock_flow_profiles = _neutralize_flow_profiles(raw_flow_profiles, kline_by_code)
    flow_quality = _flow_dataset_quality(stock_flow_profiles, kline_by_code, stock_flow_rows)
    external_score = _number((external_summary or {}).get("external_market_score"))
    if external_score is None:
        external_score, _status, _reason = _score_snapshot([dict(item) for item in external_items])
    market_style_score = float(external_score if external_score is not None else 50.0)

    scored_themes: list[dict[str, Any]] = []
    for theme in themes.values():
        family_key = str(theme["family_key"])
        catalyst, catalyst_evidence, catalyst_regions = _catalyst_score(
            family_key, str(theme["theme_name"]), selected_news, cutoff
        )
        external, external_evidence = score_external_theme(
            family_key, external_items, catalyst_regions=catalyst_regions
        )
        commodity, commodity_evidence = _commodity_score(family_key, external_items, catalyst)

        latest_hot_date = max((_date_text(row.get("snapshot_date")) for row in theme["hot_rows"]), default="")
        latest_hot = [row for row in theme["hot_rows"] if _date_text(row.get("snapshot_date")) == latest_hot_date]
        latest_flow_date = max((_date_text(row.get("snapshot_at")) for row in theme["flow_rows"]), default="")
        latest_flow = [row for row in theme["flow_rows"] if _date_text(row.get("snapshot_at")) == latest_flow_date]
        latest_market_date = max((_date_text(row.get("trade_date")) for row in theme["market_rows"]), default="")
        latest_market = [row for row in theme["market_rows"] if _date_text(row.get("trade_date")) == latest_market_date]
        latest_flow.sort(key=_flow_row_score, reverse=True)
        latest_market.sort(key=_flow_row_score, reverse=True)
        latest_flow = [row for row in latest_flow[:5] if _flow_row_score(row) >= 50.0]
        latest_market = [row for row in latest_market[:5] if _flow_row_score(row) >= 50.0]
        hot_score = max((_hot_row_score(row) for row in latest_hot), default=None)
        flow_score = max((_flow_row_score(row) for row in latest_flow), default=None)
        market_score = max((_flow_row_score(row) for row in latest_market), default=None)

        signal_labels = {
            str(row.get("concept_name") or row.get("index_name") or row.get("theme_name") or "").strip()
            for row in (*latest_hot, *latest_flow, *latest_market)
            if str(row.get("concept_name") or row.get("index_name") or row.get("theme_name") or "").strip()
        }
        signal_labels.update(
            str(row.get("theme_name") or row.get("theme_code") or "").strip()
            for row in theme["strategy_rows"]
            if str(row.get("theme_name") or row.get("theme_code") or "").strip()
        )
        exact_news_labels: set[str] = set()
        related_news_labels: list[tuple[int, int, str]] = []
        matched_news_text = " ".join(
            f"{row.get('title') or ''} {row.get('content') or ''}"
            for row in selected_news
            if family_key in (row["classification"].get("family_keys") or [])
        )
        normalized_news_text = _normalize_label(matched_news_text)
        for label in theme["labels"]:
            normalized_label = _normalize_label(label)
            if len(normalized_label) >= 3 and normalized_label in normalized_news_text:
                exact_news_labels.add(str(label))
                continue
            family = _FAMILY_BY_KEY.get(family_key)
            if family:
                matched_keyword_lengths = [
                    len(_normalize_label(keyword))
                    for keyword in family.keywords
                    if _keyword_matches(keyword, label) and _keyword_matches(keyword, matched_news_text)
                ]
                if matched_keyword_lengths:
                    related_news_labels.append((max(matched_keyword_lengths), -len(normalized_label), str(label)))
        related_news_labels.sort(reverse=True)
        active_labels = signal_labels | exact_news_labels | {
            label for _specificity, _label_length, label in related_news_labels[:6]
        }
        if len(active_labels) > 18:
            prioritized = sorted(signal_labels) + sorted(exact_news_labels)
            prioritized.extend(label for _specificity, _label_length, label in related_news_labels)
            active_labels = set(dict.fromkeys(prioritized[:18]))
        member_codes_set: set[str] = set()
        member_match_counts: dict[str, int] = defaultdict(int)
        if active_labels:
            normalized_active = {_normalize_label(label) for label in active_labels}
            for label, codes in theme["members_by_label"].items():
                normalized_label = _normalize_label(label)
                if normalized_label in normalized_active or any(
                    len(active) >= 3 and (active in normalized_label or normalized_label in active)
                    for active in normalized_active
                ):
                    member_codes_set.update(codes)
                    for code in codes:
                        member_match_counts[code] += 1
        if not member_codes_set:
            member_codes_set.update(theme["members"])
            for code in member_codes_set:
                member_match_counts[code] = 1
        member_codes = sorted(member_codes_set)
        member_kline = [kline_by_code[code] for code in member_codes if code in kline_by_code]
        member_flows = [stock_flow_by_code[code] for code in member_codes if code in stock_flow_by_code]
        member_analysis = [analysis_by_code[code] for code in member_codes if code in analysis_by_code]
        positive_breadth = (
            100.0 * sum((_number(row.get("change_pct"), 0.0) or 0.0) > 0 for row in member_kline) / len(member_kline)
            if member_kline else None
        )
        positive_flow = (
            100.0 * sum((_number(row.get("main_net_inflow"), 0.0) or 0.0) > 0 for row in member_flows) / len(member_flows)
            if member_flows else None
        )
        persistent_flow_flags = []
        for code in member_codes:
            valid_values = [
                value
                for row in stock_flow_history_by_code.get(code, [])
                if (value := _number(row.get("main_net_inflow"))) is not None
            ]
            if len(valid_values) >= 3:
                persistent_flow_flags.append(
                    valid_values[0] > 0
                    and sum(value > 0 for value in valid_values) / len(valid_values) >= 0.60
                )
        persistent_flow = (
            100.0 * sum(persistent_flow_flags) / len(persistent_flow_flags)
            if persistent_flow_flags else None
        )
        member_flow_profiles = [
            stock_flow_profiles[code]
            for code in member_codes
            if code in stock_flow_profiles and _number(stock_flow_profiles[code].get("score")) is not None
        ]
        member_flow_scores = [float(profile["score"]) for profile in member_flow_profiles]
        member_flow_strength = _safe_mean(member_flow_scores) if member_flow_scores else None
        member_flow_quality = (
            "PASS"
            if member_flow_profiles and sum(
                str(profile.get("quality_status") or "BLOCK").upper() == "PASS"
                for profile in member_flow_profiles
            ) / len(member_flow_profiles) >= 0.65
            else ("WATCH" if member_flow_profiles else "BLOCK")
        )
        domestic_flow = _weighted_average(
            (
                (hot_score, 0.22),
                (flow_score, 0.22),
                (market_score, 0.08),
                (positive_breadth, 0.15),
                (positive_flow, 0.15),
                (persistent_flow, 0.10),
                (member_flow_strength, 0.08),
            ),
            default=42.0,
        )
        avg_technical = _safe_mean((row.get("technical_score") for row in member_analysis), default=50.0)
        avg_short = _safe_mean((row.get("short_term_score") for row in member_analysis), default=50.0)
        strategy_theme_score = max(
            ((_number(row.get("raw_score"), 0.0) or 0.0) * 100.0 for row in theme["strategy_rows"]),
            default=50.0,
        )
        strategy_stock_scores: dict[str, float] = {}
        for row in theme["strategy_rows"]:
            code = str(row.get("stock_code") or "").strip().zfill(6)
            score_value = (_number(row.get("raw_score"), 0.0) or 0.0) * 100.0
            if code and score_value > strategy_stock_scores.get(code, 0.0):
                strategy_stock_scores[code] = score_value
        prior_changes = [_number(row.get("change_pct"), 0.0) or 0.0 for row in member_kline]
        not_extended_ratio = (
            100.0 * sum(-4.0 <= value <= 5.0 for value in prior_changes) / len(prior_changes)
            if prior_changes else 50.0
        )
        technical_readiness = _weighted_average(
            ((avg_technical, 0.40), (avg_short, 0.20), (not_extended_ratio, 0.20),
             (strategy_theme_score, 0.20)),
            default=50.0,
        )
        distinct_hot_dates = len({
            _date_text(row.get("snapshot_date")) for row in theme["hot_rows"] if row.get("snapshot_date")
        })
        continuity = _clamp(35.0 + distinct_hot_dates * 12.0 + (8.0 if latest_hot else 0.0))

        crowding = 0.0
        latest_hot_change = max((_number(row.get("change_pct"), 0.0) or 0.0 for row in latest_hot), default=0.0)
        if latest_hot_change > 3.0:
            crowding += min(7.0, (latest_hot_change - 3.0) * 1.5)
        if prior_changes:
            extended_ratio = sum(value >= 8.0 for value in prior_changes) / len(prior_changes)
            crowding += min(8.0, extended_ratio * 24.0)
        total_score = (
            catalyst * 0.20
            + domestic_flow * 0.20
            + external * 0.20
            + technical_readiness * 0.15
            + commodity * 0.10
            + market_style_score * 0.10
            + continuity * 0.05
            - crowding
        )
        total_score = _clamp(total_score)
        if total_score >= 72.0 and len(member_codes) >= 3:
            status = "主线候选"
        elif total_score >= 64.0:
            status = "重点观察"
        elif total_score >= 56.0:
            status = "观察"
        else:
            status = "暂缓"

        component_availability = sum([
            bool(catalyst_evidence), hot_score is not None or flow_score is not None,
            bool(external_evidence), bool(member_analysis), bool(member_codes),
        ])
        data_quality = "PASS" if component_availability >= 4 else ("WATCH" if component_availability >= 2 else "LOW")
        evidence = []
        evidence.extend(catalyst_evidence)
        evidence.extend(external_evidence)
        evidence.extend(commodity_evidence)
        if hot_score is not None:
            evidence.append(f"上一完整交易日板块热度{hot_score:.1f}")
        if positive_breadth is not None:
            evidence.append(f"成分股上涨占比{positive_breadth:.1f}%")
        if positive_flow is not None:
            evidence.append(f"成分股主力净流入占比{positive_flow:.1f}%")
        if persistent_flow is not None:
            evidence.append(f"成分股近5日持续净流入占比{persistent_flow:.1f}%")
        if member_flow_strength is not None:
            evidence.append(f"成分股资金强度{member_flow_strength:.1f}")
        if theme["strategy_rows"]:
            evidence.append(f"V3主题信号{strategy_theme_score:.1f}")

        scored = {
            "theme_key": theme["theme_key"],
            "family_key": family_key,
            "theme_name": theme["theme_name"],
            "concept_names": sorted(active_labels) if active_labels else sorted(theme["labels"])[:30],
            "score": round(total_score, 1),
            "status": status,
            "data_quality": data_quality,
            "member_count": len(member_codes),
            "catalyst_score": round(catalyst, 1),
            "flow_breadth_score": round(domestic_flow, 1),
            "flow_positive_ratio": round(positive_flow, 1) if positive_flow is not None else None,
            "flow_persistence_ratio": round(persistent_flow, 1) if persistent_flow is not None else None,
            "member_flow_strength_score": round(member_flow_strength, 1) if member_flow_strength is not None else None,
            "flow_quality_status": member_flow_quality,
            "flow_metrics": {
                "covered_members": len(member_flow_profiles),
                "member_count": len(member_codes),
                "positive_ratio": round(positive_flow, 1) if positive_flow is not None else None,
                "persistence_ratio": round(persistent_flow, 1) if persistent_flow is not None else None,
                "strength_score": round(member_flow_strength, 1) if member_flow_strength is not None else None,
            },
            "external_score": round(external, 1),
            "technical_score": round(technical_readiness, 1),
            "commodity_score": round(commodity, 1),
            "market_style_score": round(market_style_score, 1),
            "continuity_score": round(continuity, 1),
            "v3_theme_score": round(strategy_theme_score, 1),
            "crowding_penalty": round(crowding, 1),
            "evidence": evidence[:10],
            "member_codes": member_codes,
            "stock_candidates": [],
        }

        candidates = []
        for code in member_codes:
            alpha = alpha_by_code.get(code, {})
            analysis = analysis_by_code.get(code, {})
            recommendation = recommendation_by_code.get(code, {})
            kline = kline_by_code.get(code, {})
            flow = stock_flow_by_code.get(code, {})
            name = str(
                analysis.get("stock_name") or recommendation.get("short_name")
                or kline.get("short_name") or ""
            ).strip()
            if not name or "ST" in name.upper() or "退" in name:
                continue
            candidate_score, penalty, signal_status, stock_evidence = _stock_candidate_score(
                code, total_score, member_match_counts.get(code, 1),
                strategy_stock_scores.get(code), alpha,
                analysis, recommendation, kline, flow,
                stock_flow_history_by_code.get(code, ()), domestic_flow,
                stock_flow_profiles.get(code), str(flow_quality.get("status") or "BLOCK"),
            )
            flow_profile = dict(stock_flow_profiles.get(code) or {})
            resonance = _flow_resonance_score(
                _number(flow_profile.get("latest_direction")), domestic_flow
            )
            factor_score = _weighted_average(
                ((_number(flow_profile.get("score")), 0.85), (resonance, 0.15)),
                default=_number(flow_profile.get("score"), 0.0) or 0.0,
            ) if _number(flow_profile.get("score")) is not None else None
            candidates.append({
                "stock_code": code,
                "stock_name": name,
                "score": candidate_score,
                "signal_status": signal_status,
                "penalty_score": penalty,
                "previous_change_pct": round(_number(kline.get("change_pct"), 0.0) or 0.0, 2),
                "reason": "；".join(stock_evidence),
                "evidence": stock_evidence,
                "flow_factor_score": round(factor_score, 1) if factor_score is not None else None,
                "flow_quality_status": str(flow_profile.get("quality_status") or "BLOCK"),
                "flow_metrics": flow_profile,
            })
        candidates.sort(key=lambda item: (-float(item["score"]), float(item["penalty_score"]), item["stock_code"]))
        for rank, candidate in enumerate(candidates[: max(1, int(stocks_per_theme))], start=1):
            candidate["rank"] = rank
            scored["stock_candidates"].append(candidate)
        structured_domestic_signal = bool(
            latest_hot or latest_flow or latest_market or theme["strategy_rows"]
        )
        evidence_source_count = sum((
            bool(catalyst_evidence),
            structured_domestic_signal,
            bool(external_evidence),
            bool(member_analysis),
        ))
        scored["evidence_source_count"] = evidence_source_count
        if family_key == "dynamic" and not (
            len(member_codes) >= 1
            and bool(scored["stock_candidates"])
            and total_score >= 56.0
            and (structured_domestic_signal or len(catalyst_evidence) >= 2)
        ):
            continue
        scored_themes.append(scored)

    scored_themes.sort(
        key=lambda item: (-float(item["score"]), -len(item["stock_candidates"]), -int(item["member_count"]), item["theme_name"])
    )
    visible_themes = scored_themes[: max(1, int(theme_limit))]
    candidates_by_code: dict[str, dict[str, Any]] = {}
    for theme_rank, theme in enumerate(visible_themes, start=1):
        theme["rank"] = theme_rank
        for candidate in theme["stock_candidates"]:
            code = str(candidate.get("stock_code") or "")
            enriched = {
                **candidate,
                "theme_key": theme["theme_key"],
                "theme_name": theme["theme_name"],
                "theme_rank": theme_rank,
                "also_matches": [],
            }
            existing_candidate = candidates_by_code.get(code)
            if existing_candidate is None:
                candidates_by_code[code] = enriched
            elif float(enriched["score"]) > float(existing_candidate["score"]):
                enriched["also_matches"] = [
                    existing_candidate["theme_name"],
                    *(existing_candidate.get("also_matches") or []),
                ]
                candidates_by_code[code] = enriched
            elif theme["theme_name"] != existing_candidate["theme_name"]:
                existing_candidate.setdefault("also_matches", []).append(theme["theme_name"])
        theme.pop("member_codes", None)
    all_candidates = list(candidates_by_code.values())
    all_candidates.sort(key=lambda item: (-float(item["score"]), int(item["theme_rank"]), int(item["rank"])))
    for global_rank, candidate in enumerate(all_candidates, start=1):
        candidate["global_rank"] = global_rank
        candidate["also_matches"] = list(dict.fromkeys(candidate.get("also_matches") or []))

    quality_parts = {
        "news": len(selected_news),
        "memberships": len(membership_rows),
        "analysis": len(analysis_rows),
        "theme_signals": len(theme_signal_rows),
        "v3_alpha": len(alpha_rows),
        "kline": len(kline_rows),
        "stock_flow": len(stock_flow_by_code),
        "external": sum(str(item.get("availability") or "available") == "available" for item in external_items),
        "external_core": sum(
            str(item.get("availability") or "available") == "available"
            and str(item.get("symbol") or "") in {
                "nasdaq", "sp500", "dow", "nikkei", "kospi", "hang_seng", "taiwan",
            }
            for item in external_items
        ),
    }
    quality_ok = sum(value > 0 for value in quality_parts.values())
    data_quality = (
        "PASS"
        if quality_parts["external_core"] >= 3 and quality_ok >= 5
        else ("WATCH" if quality_ok >= 2 else "LOW")
    )
    if str(flow_quality.get("status") or "BLOCK") == "BLOCK":
        data_quality = "LOW"
    elif str(flow_quality.get("status") or "WATCH") == "WATCH" and data_quality == "PASS":
        data_quality = "WATCH"
    top_name = visible_themes[0]["theme_name"] if visible_themes else "暂无"
    return {
        "run_uid": str(uuid.uuid4()),
        "stage": PREMARKET_STAGE,
        "model_version": MODEL_VERSION,
        "session_date": str(session_date)[:10],
        "source_trade_date": str(source_trade_date)[:10],
        "cutoff_at": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "generated_at": datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
        "data_quality": data_quality,
        "quality_counts": {**quality_parts, "flow_quality": flow_quality},
        "flow_quality": flow_quality,
        "summary": f"09:08盘前主线首位：{top_name}；共{len(visible_themes)}个主题、{len(all_candidates)}只候选。",
        "themes": visible_themes,
        "stock_candidates": all_candidates,
    }


def _safe_rows(engine: Engine, sql: str, params: Mapping[str, Any], context: str) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in read_sql_rows(engine, sql, dict(params), context=context)]
    except Exception as exc:
        logger.warning("Premarket input skipped [%s]: %s", context, exc)
        return []


def _load_forecast_inputs(
    engine: Engine,
    *,
    source_trade_date: str,
    cutoff_at: datetime,
    external_snapshot: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source_end = datetime.combine(date.fromisoformat(source_trade_date), time.max).replace(microsecond=0)
    history_start = date.fromisoformat(source_trade_date) - timedelta(days=14)
    hot_rows = _safe_rows(engine, """
        SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name,
               change_pct, hot_value, hot_tag
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date BETWEEN :history_start AND :source_date
        ORDER BY snapshot_date DESC, `rank` ASC
    """, {"history_start": history_start, "source_date": source_trade_date}, "premarket.hot_concepts")
    concept_flow_rows = _safe_rows(engine, """
        SELECT days_type, index_code, index_name, change_pct, main_net_inflow,
               main_net_inflow_rate, snapshot_at
        FROM sm_concept_capital_flow_east
        WHERE snapshot_at <= :source_end
          AND snapshot_at >= :history_start
        ORDER BY snapshot_at DESC, days_type ASC
    """, {"source_end": source_end, "history_start": history_start}, "premarket.concept_flow")
    concept_market_rows = _safe_rows(engine, """
        SELECT c.index_code, m.name AS concept_name, c.trade_date, c.change_pct,
               c.amount, c.snapshot_at
        FROM sm_concept_east_current c
        LEFT JOIN si_concept_code_east m ON m.index_code = c.index_code
        WHERE c.trade_date <= :source_date
          AND c.trade_date >= :history_start
        ORDER BY c.trade_date DESC, c.snapshot_at DESC
    """, {"source_date": source_trade_date, "history_start": history_start}, "premarket.concept_market")
    news_rows = _safe_rows(engine, """
        SELECT source, title, content, publish_time, level, is_top, jpush
        FROM st_news_flash
        WHERE publish_time <= :cutoff_at
          AND publish_time >= :news_start
        ORDER BY publish_time DESC
        LIMIT 800
    """, {"cutoff_at": cutoff_at, "news_start": cutoff_at - timedelta(hours=60)}, "premarket.news")
    membership_rows = _safe_rows(engine, """
        SELECT stock_code, name AS concept_name, '东财概念' AS membership_source
        FROM si_stock_concept_east
        WHERE etl_sync_at <= :cutoff_at
        UNION ALL
        SELECT stock_code, plate_name AS concept_name, CONCAT('东财', plate_type) AS membership_source
        FROM si_stock_plate_east
        WHERE etl_sync_at <= :cutoff_at
    """, {"cutoff_at": cutoff_at}, "premarket.memberships")
    theme_signal_rows = _safe_rows(engine, """
        SELECT stock_code, MAX(short_name) AS short_name,
               theme_code, MAX(theme_name) AS theme_name,
               MAX(raw_score) AS raw_score,
               MAX(selected_as_primary) AS selected_as_primary,
               COUNT(DISTINCT strategy_key) AS strategy_count
        FROM st_theme_signal_v3
        WHERE trade_date = :source_date
          AND feature_time <= :source_end
          AND created_at <= :cutoff_at
        GROUP BY stock_code, theme_code
    """, {
        "source_date": source_trade_date,
        "source_end": source_end,
        "cutoff_at": cutoff_at,
    }, "premarket.theme_signals_v3")
    alpha_rows = _safe_rows(engine, """
        SELECT stock_code,
               MAX(raw_score) * 100.0 AS v3_alpha_score,
               MIN(rank_no) AS v3_best_rank,
               COUNT(DISTINCT strategy_key) AS v3_strategy_count
        FROM st_alpha_forecast_v3
        WHERE trade_date = :source_date
          AND feature_time <= :source_end
          AND created_at <= :cutoff_at
        GROUP BY stock_code
    """, {
        "source_date": source_trade_date,
        "source_end": source_end,
        "cutoff_at": cutoff_at,
    }, "premarket.alpha_v3")
    analysis_rows = _safe_rows(engine, """
        SELECT stock_code, stock_name, short_term_score, technical_score,
               capital_score, sentiment_score, event_score, event_risk_level,
               recommend_status, chase_risk_status, ordinary_buy_eligible
        FROM stock_analysis_result
        WHERE analysis_date = :source_date
    """, {"source_date": source_trade_date}, "premarket.analysis")
    recommendation_rows = _safe_rows(engine, """
        SELECT stock_code, short_name, ai_score, short_term_score, capital_score,
               technical, event_score, final_trade_score, main_wave_score,
               recommend_status, event_risk_level, chase_risk_status,
               ordinary_buy_eligible
        FROM st_recommended_stocks
        WHERE pick_date = :source_date
    """, {"source_date": source_trade_date}, "premarket.recommendations")
    kline_rows = _safe_rows(engine, """
        SELECT k.stock_code, k.short_name, k.close, k.change_pct,
               k.turnover_ratio, k.amount,
               k.close * shares.total_shares AS market_cap,
               industry.industry_name
        FROM sm_stock_kline k
        LEFT JOIN (
            SELECT stock_code, MAX(total_shares) AS total_shares
            FROM si_stock_shares GROUP BY stock_code
        ) shares ON shares.stock_code = k.stock_code
        LEFT JOIN (
            SELECT stock_code, MAX(industry_name) AS industry_name
            FROM si_industry_sw
            WHERE industry_type = '申万一级'
            GROUP BY stock_code
        ) industry ON industry.stock_code = k.stock_code
        WHERE k.trade_date = :source_date AND k.k_type = 1 AND k.adjust_type = 0
    """, {"source_date": source_trade_date}, "premarket.kline")
    stock_flow_rows = _safe_rows(engine, """
        SELECT flow.stock_code, flow.trade_date, flow.main_net_inflow,
               flow.max_net_inflow, flow.lg_net_inflow,
               kline.amount AS turnover_amount
        FROM sm_stock_capital_flow_daily flow
        LEFT JOIN sm_stock_kline kline
          ON kline.stock_code = flow.stock_code
         AND kline.trade_date = flow.trade_date
         AND kline.k_type = 1
         AND kline.adjust_type = 0
        WHERE flow.trade_date BETWEEN :history_start AND :source_date
        ORDER BY flow.stock_code ASC, flow.trade_date DESC
    """, {
        "history_start": history_start,
        "source_date": source_trade_date,
    }, "premarket.stock_flow")

    if external_snapshot is not None:
        external_items = [dict(item) for item in external_snapshot.get("items") or []]
        external_summary = dict(external_snapshot)
    else:
        external_summary = load_latest_external_market_context(engine, cutoff_at)
        try:
            external_items = json.loads(str(external_summary.get("external_market_items_json") or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            external_items = []
    return {
        "hot_rows": hot_rows,
        "concept_flow_rows": concept_flow_rows,
        "concept_market_rows": concept_market_rows,
        "news_rows": news_rows,
        "membership_rows": membership_rows,
        "theme_signal_rows": theme_signal_rows,
        "alpha_rows": alpha_rows,
        "analysis_rows": analysis_rows,
        "recommendation_rows": recommendation_rows,
        "kline_rows": kline_rows,
        "stock_flow_rows": stock_flow_rows,
        "external_items": external_items,
        "external_summary": external_summary,
    }


_RUN_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_premarket_theme_forecast_run (
    id BIGINT NOT NULL AUTO_INCREMENT,
    run_uid VARCHAR(64) NOT NULL,
    session_date DATE NOT NULL,
    source_trade_date DATE NOT NULL,
    cutoff_at DATETIME NOT NULL,
    stage VARCHAR(32) NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,
    data_quality VARCHAR(16) NOT NULL,
    theme_count INT NOT NULL DEFAULT 0,
    candidate_count INT NOT NULL DEFAULT 0,
    top_theme VARCHAR(256) NULL,
    summary VARCHAR(1000) NULL,
    quality_json LONGTEXT NULL,
    delivery_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    delivery_id VARCHAR(64) NULL,
    delivery_error VARCHAR(512) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_premarket_theme_run_uid (run_uid),
    UNIQUE KEY uk_premarket_theme_session_stage (session_date, stage),
    KEY idx_premarket_theme_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_ITEM_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_premarket_theme_forecast_item (
    id BIGINT NOT NULL AUTO_INCREMENT,
    run_uid VARCHAR(64) NOT NULL,
    rank_no INT NOT NULL,
    theme_key VARCHAR(160) NOT NULL,
    family_key VARCHAR(64) NOT NULL,
    theme_name VARCHAR(256) NOT NULL,
    total_score DECIMAL(8,2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    data_quality VARCHAR(16) NOT NULL,
    member_count INT NOT NULL DEFAULT 0,
    catalyst_score DECIMAL(8,2) NOT NULL,
    flow_breadth_score DECIMAL(8,2) NOT NULL,
    flow_positive_ratio DECIMAL(8,2) NULL,
    flow_persistence_ratio DECIMAL(8,2) NULL,
    member_flow_strength_score DECIMAL(8,2) NULL,
    flow_quality_status VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',
    flow_metrics_json LONGTEXT NULL,
    external_score DECIMAL(8,2) NOT NULL,
    technical_score DECIMAL(8,2) NOT NULL,
    commodity_score DECIMAL(8,2) NOT NULL,
    market_style_score DECIMAL(8,2) NOT NULL,
    continuity_score DECIMAL(8,2) NOT NULL,
    crowding_penalty DECIMAL(8,2) NOT NULL,
    concept_names_json LONGTEXT NULL,
    evidence_json LONGTEXT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_premarket_theme_item (run_uid, theme_key),
    KEY idx_premarket_theme_item_rank (run_uid, rank_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_STOCK_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS st_premarket_theme_stock_candidate (
    id BIGINT NOT NULL AUTO_INCREMENT,
    run_uid VARCHAR(64) NOT NULL,
    global_rank INT NOT NULL,
    theme_rank INT NOT NULL,
    stock_rank INT NOT NULL,
    theme_key VARCHAR(160) NOT NULL,
    theme_name VARCHAR(256) NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    stock_name VARCHAR(128) NOT NULL,
    candidate_score DECIMAL(8,2) NOT NULL,
    signal_status VARCHAR(32) NOT NULL,
    penalty_score DECIMAL(8,2) NOT NULL,
    previous_change_pct DECIMAL(10,4) NULL,
    flow_factor_score DECIMAL(8,2) NULL,
    flow_quality_status VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',
    flow_metrics_json LONGTEXT NULL,
    reason VARCHAR(1000) NULL,
    evidence_json LONGTEXT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_premarket_theme_stock (run_uid, theme_key, stock_code),
    KEY idx_premarket_theme_stock_rank (run_uid, global_rank),
    KEY idx_premarket_theme_stock_code (stock_code, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


_FORECAST_COLUMN_MIGRATIONS: dict[str, dict[str, str]] = {
    "st_premarket_theme_forecast_item": {
        "flow_positive_ratio": "DECIMAL(8,2) NULL",
        "flow_persistence_ratio": "DECIMAL(8,2) NULL",
        "member_flow_strength_score": "DECIMAL(8,2) NULL",
        "flow_quality_status": "VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN'",
        "flow_metrics_json": "LONGTEXT NULL",
    },
    "st_premarket_theme_stock_candidate": {
        "flow_factor_score": "DECIMAL(8,2) NULL",
        "flow_quality_status": "VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN'",
        "flow_metrics_json": "LONGTEXT NULL",
    },
}


def _ensure_forecast_columns(connection: Any) -> None:
    """Apply additive MySQL migrations without touching frozen forecast rows."""

    try:
        rows = connection.execute(text("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN (
                  'st_premarket_theme_forecast_item',
                  'st_premarket_theme_stock_candidate'
              )
        """)).mappings().all()
    except Exception as exc:
        logger.warning("Premarket forecast column audit skipped: %s", exc)
        return
    existing = {
        (str(row.get("TABLE_NAME") or row.get("table_name") or ""),
         str(row.get("COLUMN_NAME") or row.get("column_name") or ""))
        for row in rows
    }
    for table_name, columns in _FORECAST_COLUMN_MIGRATIONS.items():
        for column_name, definition in columns.items():
            if (table_name, column_name) in existing:
                continue
            connection.execute(text(
                f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {definition}"
            ))


def ensure_premarket_theme_tables(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(_RUN_TABLE_DDL))
        connection.execute(text(_ITEM_TABLE_DDL))
        connection.execute(text(_STOCK_TABLE_DDL))
        _ensure_forecast_columns(connection)


def persist_premarket_theme_forecast(engine: Engine, forecast: Mapping[str, Any]) -> dict[str, Any]:
    """Persist the first completed 09:08 result and never overwrite it."""

    ensure_premarket_theme_tables(engine)
    session_date = str(forecast.get("session_date") or "")[:10]
    stage = str(forecast.get("stage") or PREMARKET_STAGE)
    with engine.begin() as connection:
        existing = connection.execute(text("""
            SELECT run_uid, delivery_status, delivery_id
            FROM st_premarket_theme_forecast_run
            WHERE session_date = :session_date AND stage = :stage
            LIMIT 1
        """), {"session_date": session_date, "stage": stage}).mappings().first()
        if existing:
            return {
                "created": False,
                "run_uid": str(existing.get("run_uid") or ""),
                "delivery_status": str(existing.get("delivery_status") or "PENDING"),
                "delivery_id": str(existing.get("delivery_id") or ""),
            }

        run_uid = str(forecast.get("run_uid") or uuid.uuid4())
        now = datetime.now().replace(microsecond=0)
        themes = list(forecast.get("themes") or [])
        candidates = list(forecast.get("stock_candidates") or [])
        connection.execute(text("""
            INSERT INTO st_premarket_theme_forecast_run
                (run_uid, session_date, source_trade_date, cutoff_at, stage,
                 model_version, status, data_quality, theme_count, candidate_count,
                 top_theme, summary, quality_json, delivery_status, created_at, updated_at)
            VALUES
                (:run_uid, :session_date, :source_trade_date, :cutoff_at, :stage,
                 :model_version, 'COMPLETED', :data_quality, :theme_count, :candidate_count,
                 :top_theme, :summary, :quality_json, 'PENDING', :created_at, :updated_at)
        """), {
            "run_uid": run_uid,
            "session_date": session_date,
            "source_trade_date": str(forecast.get("source_trade_date") or "")[:10],
            "cutoff_at": _parse_datetime(forecast.get("cutoff_at")),
            "stage": stage,
            "model_version": str(forecast.get("model_version") or MODEL_VERSION),
            "data_quality": str(forecast.get("data_quality") or "UNKNOWN"),
            "theme_count": len(themes),
            "candidate_count": len(candidates),
            "top_theme": str(themes[0].get("theme_name") or "")[:256] if themes else "",
            "summary": str(forecast.get("summary") or "")[:1000],
            "quality_json": json.dumps(forecast.get("quality_counts") or {}, ensure_ascii=False, default=str),
            "created_at": now,
            "updated_at": now,
        })
        if themes:
            connection.execute(text("""
                INSERT INTO st_premarket_theme_forecast_item
                    (run_uid, rank_no, theme_key, family_key, theme_name, total_score,
                     status, data_quality, member_count, catalyst_score, flow_breadth_score,
                     flow_positive_ratio, flow_persistence_ratio, member_flow_strength_score,
                     flow_quality_status, flow_metrics_json,
                     external_score, technical_score, commodity_score, market_style_score,
                     continuity_score, crowding_penalty, concept_names_json, evidence_json, created_at)
                VALUES
                    (:run_uid, :rank_no, :theme_key, :family_key, :theme_name, :total_score,
                     :status, :data_quality, :member_count, :catalyst_score, :flow_breadth_score,
                     :flow_positive_ratio, :flow_persistence_ratio, :member_flow_strength_score,
                     :flow_quality_status, :flow_metrics_json,
                     :external_score, :technical_score, :commodity_score, :market_style_score,
                     :continuity_score, :crowding_penalty, :concept_names_json, :evidence_json, :created_at)
            """), [{
                "run_uid": run_uid,
                "rank_no": int(theme.get("rank") or 0),
                "theme_key": str(theme.get("theme_key") or "")[:160],
                "family_key": str(theme.get("family_key") or "dynamic")[:64],
                "theme_name": str(theme.get("theme_name") or "")[:256],
                "total_score": _number(theme.get("score"), 0.0),
                "status": str(theme.get("status") or "观察")[:32],
                "data_quality": str(theme.get("data_quality") or "UNKNOWN")[:16],
                "member_count": int(theme.get("member_count") or 0),
                "catalyst_score": _number(theme.get("catalyst_score"), 0.0),
                "flow_breadth_score": _number(theme.get("flow_breadth_score"), 0.0),
                "flow_positive_ratio": _number(theme.get("flow_positive_ratio")),
                "flow_persistence_ratio": _number(theme.get("flow_persistence_ratio")),
                "member_flow_strength_score": _number(theme.get("member_flow_strength_score")),
                "flow_quality_status": str(theme.get("flow_quality_status") or "UNKNOWN")[:16],
                "flow_metrics_json": json.dumps(theme.get("flow_metrics") or {}, ensure_ascii=False, default=str),
                "external_score": _number(theme.get("external_score"), 0.0),
                "technical_score": _number(theme.get("technical_score"), 0.0),
                "commodity_score": _number(theme.get("commodity_score"), 0.0),
                "market_style_score": _number(theme.get("market_style_score"), 0.0),
                "continuity_score": _number(theme.get("continuity_score"), 0.0),
                "crowding_penalty": _number(theme.get("crowding_penalty"), 0.0),
                "concept_names_json": json.dumps(theme.get("concept_names") or [], ensure_ascii=False),
                "evidence_json": json.dumps(theme.get("evidence") or [], ensure_ascii=False, default=str),
                "created_at": now,
            } for theme in themes])
        if candidates:
            connection.execute(text("""
                INSERT INTO st_premarket_theme_stock_candidate
                    (run_uid, global_rank, theme_rank, stock_rank, theme_key, theme_name,
                     stock_code, stock_name, candidate_score, signal_status, penalty_score,
                     previous_change_pct, flow_factor_score, flow_quality_status,
                     flow_metrics_json, reason, evidence_json, created_at)
                VALUES
                    (:run_uid, :global_rank, :theme_rank, :stock_rank, :theme_key, :theme_name,
                     :stock_code, :stock_name, :candidate_score, :signal_status, :penalty_score,
                     :previous_change_pct, :flow_factor_score, :flow_quality_status,
                     :flow_metrics_json, :reason, :evidence_json, :created_at)
            """), [{
                "run_uid": run_uid,
                "global_rank": int(item.get("global_rank") or 0),
                "theme_rank": int(item.get("theme_rank") or 0),
                "stock_rank": int(item.get("rank") or 0),
                "theme_key": str(item.get("theme_key") or "")[:160],
                "theme_name": str(item.get("theme_name") or "")[:256],
                "stock_code": str(item.get("stock_code") or "")[:16],
                "stock_name": str(item.get("stock_name") or "")[:128],
                "candidate_score": _number(item.get("score"), 0.0),
                "signal_status": str(item.get("signal_status") or "观察")[:32],
                "penalty_score": _number(item.get("penalty_score"), 0.0),
                "previous_change_pct": _number(item.get("previous_change_pct")),
                "flow_factor_score": _number(item.get("flow_factor_score")),
                "flow_quality_status": str(item.get("flow_quality_status") or "UNKNOWN")[:16],
                "flow_metrics_json": json.dumps(item.get("flow_metrics") or {}, ensure_ascii=False, default=str),
                "reason": str(item.get("reason") or "")[:1000],
                "evidence_json": json.dumps(item.get("evidence") or [], ensure_ascii=False, default=str),
                "created_at": now,
            } for item in candidates])
    return {"created": True, "run_uid": run_uid, "delivery_status": "PENDING", "delivery_id": ""}


def mark_forecast_delivery(
    engine: Engine,
    run_uid: str,
    *,
    status: str,
    delivery_id: str = "",
    error: str = "",
) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_premarket_theme_forecast_run
            SET delivery_status = :status, delivery_id = :delivery_id,
                delivery_error = :delivery_error, updated_at = :updated_at
            WHERE run_uid = :run_uid
        """), {
            "run_uid": run_uid,
            "status": str(status or "UNKNOWN")[:16],
            "delivery_id": str(delivery_id or "")[:64] or None,
            "delivery_error": str(error or "")[:512] or None,
            "updated_at": datetime.now().replace(microsecond=0),
        })


def load_premarket_theme_forecast(
    engine: Engine,
    session_date: str | None = None,
    *,
    stage: str = PREMARKET_STAGE,
    allow_fallback: bool = True,
) -> dict[str, Any]:
    requested = str(session_date or "")[:10]
    try:
        params: dict[str, Any] = {"stage": stage}
        where = "stage = :stage AND status = 'COMPLETED'"
        if requested:
            where += " AND session_date = :session_date"
            params["session_date"] = requested
        run_rows = _safe_rows(engine, f"""
            SELECT * FROM st_premarket_theme_forecast_run
            WHERE {where}
            ORDER BY session_date DESC, created_at ASC
            LIMIT 1
        """, params, "premarket.load_run")
        fallback = False
        if not run_rows and requested and allow_fallback:
            run_rows = _safe_rows(engine, """
                SELECT * FROM st_premarket_theme_forecast_run
                WHERE stage = :stage AND status = 'COMPLETED' AND session_date <= :session_date
                ORDER BY session_date DESC, created_at ASC
                LIMIT 1
            """, {"stage": stage, "session_date": requested}, "premarket.load_run_fallback")
            fallback = bool(run_rows)
        if not run_rows:
            return {
                "requested_date": requested,
                "session_date": requested,
                "stage": stage,
                "fallback": False,
                "themes": [],
                "stock_candidates": [],
                "total": 0,
            }
        run = run_rows[0]
        run_uid = str(run.get("run_uid") or "")
        try:
            quality_counts = json.loads(str(run.get("quality_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            quality_counts = {}
        theme_rows = _safe_rows(engine, """
            SELECT * FROM st_premarket_theme_forecast_item
            WHERE run_uid = :run_uid ORDER BY rank_no ASC
        """, {"run_uid": run_uid}, "premarket.load_themes")
        stock_rows = _safe_rows(engine, """
            SELECT * FROM st_premarket_theme_stock_candidate
            WHERE run_uid = :run_uid ORDER BY global_rank ASC
        """, {"run_uid": run_uid}, "premarket.load_stocks")
        themes = []
        for row in theme_rows:
            try:
                concepts = json.loads(str(row.get("concept_names_json") or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                concepts = []
            try:
                evidence = json.loads(str(row.get("evidence_json") or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                evidence = []
            try:
                flow_metrics = json.loads(str(row.get("flow_metrics_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                flow_metrics = {}
            themes.append({
                "rank": int(row.get("rank_no") or 0),
                "theme_key": row.get("theme_key"),
                "family_key": row.get("family_key"),
                "theme_name": row.get("theme_name"),
                "score": _number(row.get("total_score"), 0.0),
                "status": row.get("status"),
                "data_quality": row.get("data_quality"),
                "member_count": int(row.get("member_count") or 0),
                "catalyst_score": _number(row.get("catalyst_score"), 0.0),
                "flow_breadth_score": _number(row.get("flow_breadth_score"), 0.0),
                "flow_positive_ratio": _number(row.get("flow_positive_ratio")),
                "flow_persistence_ratio": _number(row.get("flow_persistence_ratio")),
                "member_flow_strength_score": _number(row.get("member_flow_strength_score")),
                "flow_quality_status": row.get("flow_quality_status") or "UNKNOWN",
                "flow_metrics": flow_metrics,
                "external_score": _number(row.get("external_score"), 0.0),
                "technical_score": _number(row.get("technical_score"), 0.0),
                "commodity_score": _number(row.get("commodity_score"), 0.0),
                "market_style_score": _number(row.get("market_style_score"), 0.0),
                "continuity_score": _number(row.get("continuity_score"), 0.0),
                "crowding_penalty": _number(row.get("crowding_penalty"), 0.0),
                "concept_names": concepts,
                "evidence": evidence,
                "stock_candidates": [],
            })
        theme_map = {str(item["theme_key"]): item for item in themes}
        candidates = []
        for row in stock_rows:
            try:
                evidence = json.loads(str(row.get("evidence_json") or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                evidence = []
            try:
                flow_metrics = json.loads(str(row.get("flow_metrics_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                flow_metrics = {}
            item = {
                "global_rank": int(row.get("global_rank") or 0),
                "theme_rank": int(row.get("theme_rank") or 0),
                "rank": int(row.get("stock_rank") or 0),
                "theme_key": row.get("theme_key"),
                "theme_name": row.get("theme_name"),
                "stock_code": row.get("stock_code"),
                "stock_name": row.get("stock_name"),
                "score": _number(row.get("candidate_score"), 0.0),
                "signal_status": row.get("signal_status"),
                "penalty_score": _number(row.get("penalty_score"), 0.0),
                "previous_change_pct": _number(row.get("previous_change_pct"), 0.0),
                "flow_factor_score": _number(row.get("flow_factor_score")),
                "flow_quality_status": row.get("flow_quality_status") or "UNKNOWN",
                "flow_metrics": flow_metrics,
                "reason": row.get("reason") or "",
                "evidence": evidence,
            }
            candidates.append(item)
            if str(item["theme_key"]) in theme_map:
                theme_map[str(item["theme_key"])]["stock_candidates"].append(item)
        return {
            "requested_date": requested,
            "session_date": _date_text(run.get("session_date")),
            "source_trade_date": _date_text(run.get("source_trade_date")),
            "cutoff_at": str(run.get("cutoff_at") or "")[:19],
            "generated_at": str(run.get("created_at") or "")[:19],
            "stage": run.get("stage"),
            "model_version": run.get("model_version"),
            "data_quality": run.get("data_quality"),
            "quality_counts": quality_counts,
            "flow_quality": quality_counts.get("flow_quality") or {},
            "summary": run.get("summary") or "",
            "delivery_status": run.get("delivery_status"),
            "delivery_id": run.get("delivery_id") or "",
            "fallback": fallback,
            "themes": themes,
            "stock_candidates": candidates,
            "total": len(candidates),
            "run_uid": run_uid,
        }
    except Exception as exc:
        logger.warning("Premarket forecast unavailable: %s", exc)
        return {
            "requested_date": requested,
            "session_date": requested,
            "stage": stage,
            "fallback": False,
            "themes": [],
            "stock_candidates": [],
            "total": 0,
            "error": str(exc),
        }


def format_forecast_markdown(forecast: Mapping[str, Any]) -> str:
    session_date = str(forecast.get("session_date") or "")
    source_date = str(forecast.get("source_trade_date") or "")
    cutoff_at = str(forecast.get("cutoff_at") or "")[:19]
    lines = [
        f"## 🧭 09:08盘前主线预判｜{session_date}",
        f"> 数据截止 {cutoff_at}；A股行情/资金只使用 {source_date} 及以前完整交易日，结果已冻结。",
        "",
    ]
    themes = list(forecast.get("themes") or [])
    if not themes:
        lines.append("当前数据门禁下没有形成可发布的主题候选，请检查数据质量后再决策。")
        return "\n".join(lines)
    for theme in themes[:6]:
        lines.append(
            f"**{int(theme.get('rank') or 0)}. {theme.get('theme_name')}** "
            f"`{float(theme.get('score') or 0):.1f}`｜{theme.get('status')}｜"
            f"外盘{float(theme.get('external_score') or 0):.1f}｜"
            f"资金广度{float(theme.get('flow_breadth_score') or 0):.1f}｜"
            f"技术准备{float(theme.get('technical_score') or 0):.1f}"
        )
        evidence = list(theme.get("evidence") or [])
        if evidence:
            lines.append("- 依据：" + "；".join(str(item) for item in evidence[:3]))
        stocks = list(theme.get("stock_candidates") or [])
        if stocks:
            lines.append("- 候选：" + "；".join(
                f"{item.get('stock_name')}({item.get('stock_code')}) {float(item.get('score') or 0):.1f} {item.get('signal_status')}"
                for item in stocks[:5]
            ))
        else:
            lines.append("- 候选：当前成分股没有通过盘前风险与追高门禁")
        lines.append("")
    lines.extend([
        "**执行口径**",
        "- 09:08 是盘前预测，不使用当日集合竞价、分时或收盘后的未来数据。",
        "- 09:25/09:32 只能追加确认或否决，不能回写覆盖这份盘前结果。",
        "- 候选不等于无条件买入；高开、涨停封死、风险门禁触发时继续放弃。",
    ])
    return "\n".join(lines)


def run_premarket_theme_forecast(
    engine: Engine,
    *,
    session_date: str,
    source_trade_date: str,
    cutoff_at: datetime | str,
    external_snapshot: Mapping[str, Any] | None = None,
    theme_limit: int = DEFAULT_THEME_LIMIT,
    stocks_per_theme: int = DEFAULT_STOCKS_PER_THEME,
    persist: bool = True,
) -> dict[str, Any]:
    cutoff = _parse_datetime(cutoff_at)
    if cutoff is None:
        raise ValueError("cutoff_at is invalid")
    if persist:
        frozen = load_premarket_theme_forecast(
            engine,
            str(session_date)[:10],
            allow_fallback=False,
        )
        if frozen.get("run_uid"):
            frozen["persistence"] = {
                "created": False,
                "run_uid": frozen.get("run_uid"),
                "delivery_status": frozen.get("delivery_status") or "PENDING",
                "delivery_id": frozen.get("delivery_id") or "",
            }
            return frozen
    inputs = _load_forecast_inputs(
        engine,
        source_trade_date=str(source_trade_date)[:10],
        cutoff_at=cutoff,
        external_snapshot=external_snapshot,
    )
    forecast = build_theme_forecast_from_records(
        session_date=str(session_date)[:10],
        source_trade_date=str(source_trade_date)[:10],
        cutoff_at=cutoff,
        theme_limit=theme_limit,
        stocks_per_theme=stocks_per_theme,
        **inputs,
    )
    if persist:
        persistence = persist_premarket_theme_forecast(engine, forecast)
        forecast["persistence"] = persistence
        if not persistence.get("created"):
            frozen = load_premarket_theme_forecast(engine, str(session_date)[:10], allow_fallback=False)
            frozen["persistence"] = persistence
            return frozen
        forecast["run_uid"] = str(persistence.get("run_uid") or forecast["run_uid"])
    return forecast


_AUCTION_CONFIRMATION_DDL = """
CREATE TABLE IF NOT EXISTS st_premarket_theme_stock_confirmation (
    id BIGINT NOT NULL AUTO_INCREMENT,
    confirmation_uid VARCHAR(64) NOT NULL,
    session_date DATE NOT NULL,
    source_run_uid VARCHAR(64) NOT NULL,
    cutoff_at DATETIME NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    original_rank INT NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    stock_name VARCHAR(128) NOT NULL,
    original_score DECIMAL(8,2) NOT NULL,
    quote_price DECIMAL(18,4) NULL,
    quote_change_pct DECIMAL(10,4) NULL,
    quote_snapshot_at DATETIME NULL,
    confirmation_status VARCHAR(32) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    metrics_json LONGTEXT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_premarket_theme_confirmation (source_run_uid, stock_code),
    KEY idx_premarket_theme_confirmation_date (session_date, original_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def build_auction_confirmation_from_records(
    forecast: Mapping[str, Any],
    quote_rows: Sequence[Mapping[str, Any]],
    *,
    cutoff_at: datetime | str,
) -> dict[str, Any]:
    """Build an independent 09:20 audit layer without mutating 09:08 output."""

    cutoff = _parse_datetime(cutoff_at)
    if cutoff is None:
        raise ValueError("cutoff_at is invalid")
    session_date = str(forecast.get("session_date") or "")[:10]
    if not session_date or cutoff.date().isoformat() != session_date:
        raise ValueError("confirmation cutoff and frozen session_date must match")

    latest_quotes: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for row in quote_rows:
        code = str(row.get("stock_code") or "").zfill(6)
        snapshot = _parse_datetime(row.get("snapshot_at") or row.get("received_at"))
        if not re.fullmatch(r"[036]\d{5}", code) or snapshot is None:
            continue
        if snapshot.date().isoformat() != session_date or snapshot > cutoff:
            continue
        existing = latest_quotes.get(code)
        if existing is None or snapshot > existing[0]:
            latest_quotes[code] = (snapshot, dict(row))

    confirmations: list[dict[str, Any]] = []
    for candidate in list(forecast.get("stock_candidates") or []):
        code = str(candidate.get("stock_code") or "").zfill(6)
        quoted = latest_quotes.get(code)
        price: float | None = None
        change_pct: float | None = None
        snapshot: datetime | None = None
        if quoted is None:
            status, reason = "DATA_BLOCKED", "09:20前没有当日有效行情，不得把缺数据当确认"
        else:
            snapshot, quote = quoted
            price = _number(quote.get("price") or quote.get("latest_price"))
            change_pct = _number(quote.get("change_pct"))
            permission = str(quote.get("permission_status") or "AVAILABLE").upper()
            quality = str(quote.get("quality_status") or "PASS").upper()
            age_seconds = max(0.0, (cutoff - snapshot).total_seconds())
            if price is None or price <= 0 or change_pct is None:
                status, reason = "DATA_BLOCKED", "行情价格或涨跌幅缺失"
            elif age_seconds > 600 or permission not in {"AVAILABLE", "OK", "PASS", "SUPPORTED"} or quality in {"FAIL", "BLOCK"}:
                status, reason = "DATA_BLOCKED", "行情过期或行情质量门禁未通过"
            elif change_pct >= 9.5:
                status, reason = "REJECT_LIMIT", "已接近涨停，禁止追入"
            elif change_pct >= 5.0:
                status, reason = "REJECT_CHASE", "竞价涨幅过高，等待开盘后重新确认"
            elif change_pct <= -4.0:
                status, reason = "REJECT_WEAK", "竞价显著走弱，盘前候选被否决"
            elif -1.5 <= change_pct <= 4.0:
                status, reason = "CONFIRMED", "竞价价格可执行且未触发追高/走弱门禁"
            else:
                status, reason = "WATCH", "竞价处于边界区，仅保留观察"
        confirmations.append({
            "original_rank": int(candidate.get("global_rank") or candidate.get("rank") or 0),
            "stock_code": code,
            "stock_name": str(candidate.get("stock_name") or ""),
            "original_score": round(_number(candidate.get("score"), 0.0) or 0.0, 1),
            "quote_price": round(price, 4) if price is not None else None,
            "quote_change_pct": round(change_pct, 4) if change_pct is not None else None,
            "quote_snapshot_at": snapshot.strftime("%Y-%m-%d %H:%M:%S") if snapshot else None,
            "confirmation_status": status,
            "reason": reason,
            "metrics": {
                "source_stage": forecast.get("stage"),
                "source_model_version": forecast.get("model_version"),
                "flow_factor_score": candidate.get("flow_factor_score"),
                "flow_quality_status": candidate.get("flow_quality_status"),
            },
        })
    return {
        "confirmation_uid": str(uuid.uuid4()),
        "session_date": session_date,
        "source_run_uid": str(forecast.get("run_uid") or ""),
        "cutoff_at": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "model_version": f"{MODEL_VERSION}_AUCTION_0920",
        "confirmations": confirmations,
        "counts": dict((status, sum(item["confirmation_status"] == status for item in confirmations)) for status in (
            "CONFIRMED", "WATCH", "REJECT_CHASE", "REJECT_LIMIT", "REJECT_WEAK", "DATA_BLOCKED"
        )),
    }


def persist_auction_confirmation(engine: Engine, confirmation: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze the first 09:20 confirmation; never update 09:08 or an earlier confirmation."""

    rows = list(confirmation.get("confirmations") or [])
    source_run_uid = str(confirmation.get("source_run_uid") or "")
    if not source_run_uid:
        raise ValueError("source_run_uid is required")
    with engine.begin() as connection:
        connection.execute(text(_AUCTION_CONFIRMATION_DDL))
        existing = connection.execute(text("""
            SELECT confirmation_uid, COUNT(*) AS row_count
            FROM st_premarket_theme_stock_confirmation
            WHERE source_run_uid = :source_run_uid
            GROUP BY confirmation_uid
            LIMIT 1
        """), {"source_run_uid": source_run_uid}).mappings().first()
        if existing:
            return {
                "created": False,
                "confirmation_uid": str(existing.get("confirmation_uid") or ""),
                "row_count": int(existing.get("row_count") or 0),
            }
        if rows:
            now = datetime.now().replace(microsecond=0)
            connection.execute(text("""
                INSERT INTO st_premarket_theme_stock_confirmation
                    (confirmation_uid, session_date, source_run_uid, cutoff_at, model_version,
                     original_rank, stock_code, stock_name, original_score,
                     quote_price, quote_change_pct, quote_snapshot_at,
                     confirmation_status, reason, metrics_json, created_at)
                VALUES
                    (:confirmation_uid, :session_date, :source_run_uid, :cutoff_at, :model_version,
                     :original_rank, :stock_code, :stock_name, :original_score,
                     :quote_price, :quote_change_pct, :quote_snapshot_at,
                     :confirmation_status, :reason, :metrics_json, :created_at)
            """), [{
                "confirmation_uid": str(confirmation.get("confirmation_uid") or ""),
                "session_date": str(confirmation.get("session_date") or "")[:10],
                "source_run_uid": source_run_uid,
                "cutoff_at": _parse_datetime(confirmation.get("cutoff_at")),
                "model_version": str(confirmation.get("model_version") or "")[:64],
                "original_rank": int(item.get("original_rank") or 0),
                "stock_code": str(item.get("stock_code") or "")[:16],
                "stock_name": str(item.get("stock_name") or "")[:128],
                "original_score": _number(item.get("original_score"), 0.0),
                "quote_price": _number(item.get("quote_price")),
                "quote_change_pct": _number(item.get("quote_change_pct")),
                "quote_snapshot_at": _parse_datetime(item.get("quote_snapshot_at")),
                "confirmation_status": str(item.get("confirmation_status") or "DATA_BLOCKED")[:32],
                "reason": str(item.get("reason") or "")[:512],
                "metrics_json": json.dumps(item.get("metrics") or {}, ensure_ascii=False, default=str),
                "created_at": now,
            } for item in rows])
    return {
        "created": True,
        "confirmation_uid": str(confirmation.get("confirmation_uid") or ""),
        "row_count": len(rows),
    }


def run_auction_confirmation(
    engine: Engine,
    *,
    session_date: str,
    cutoff_at: datetime | str,
    persist: bool = True,
) -> dict[str, Any]:
    forecast = load_premarket_theme_forecast(
        engine, str(session_date)[:10], allow_fallback=False
    )
    if not forecast.get("run_uid"):
        raise RuntimeError("当天09:08冻结预判不存在，不能凭空生成09:20确认")
    codes = sorted({
        str(item.get("stock_code") or "").zfill(6)
        for item in forecast.get("stock_candidates") or []
        if re.fullmatch(r"[036]\d{5}", str(item.get("stock_code") or "").zfill(6))
    })
    params: dict[str, Any] = {"cutoff_at": _parse_datetime(cutoff_at)}
    placeholders = []
    for index, code in enumerate(codes):
        key = f"code_{index}"
        params[key] = code
        placeholders.append(f":{key}")
    quote_rows = _safe_rows(engine, f"""
        SELECT stock_code, short_name, price, change_pct, snapshot_at, received_at,
               permission_status, quality_status, data_source
        FROM sm_stock_current
        WHERE snapshot_at <= :cutoff_at
          AND stock_code IN ({', '.join(placeholders) if placeholders else "''"})
        ORDER BY stock_code ASC, snapshot_at DESC
    """, params, "premarket.auction_confirmation")
    result = build_auction_confirmation_from_records(
        forecast, quote_rows, cutoff_at=cutoff_at
    )
    if persist:
        result["persistence"] = persist_auction_confirmation(engine, result)
    return result


def format_auction_confirmation_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        f"## ⏱️ 09:20竞价确认｜{result.get('session_date')}",
        "> 这是对09:08冻结候选的独立确认，不会改写原始排名。",
        "",
    ]
    labels = {
        "CONFIRMED": "确认",
        "WATCH": "观察",
        "REJECT_CHASE": "拒绝追高",
        "REJECT_LIMIT": "拒绝涨停追入",
        "REJECT_WEAK": "否决走弱",
        "DATA_BLOCKED": "数据不足",
    }
    for item in list(result.get("confirmations") or [])[:12]:
        change = _number(item.get("quote_change_pct"))
        change_text = f"{change:+.2f}%" if change is not None else "无有效行情"
        status = str(item.get("confirmation_status") or "DATA_BLOCKED")
        lines.append(
            f"- {int(item.get('original_rank') or 0)}. {item.get('stock_name')}({item.get('stock_code')}) "
            f"{change_text}｜**{labels.get(status, status)}**｜{item.get('reason')}"
        )
    if not result.get("confirmations"):
        lines.append("- 09:08没有冻结候选，本档没有可确认标的。")
    return "\n".join(lines)
