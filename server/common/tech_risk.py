# -*- coding: utf-8 -*-
"""Black-swan sector risk radar.

The original trigger was "Meta may rent/sell excess AI compute", but the real
job is broader: turn any severe news event into affected sectors, affected
holdings, and a visible risk-control action.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Callable, Iterable


QueryFn = Callable[[str, dict | None], list[dict]]
logger = logging.getLogger(__name__)


SELL_OFF_TERMS = [
    "暴跌", "大跌", "重挫", "跳水", "杀跌", "杀估值", "下挫", "下跌", "回调",
    "tumble", "tumbled", "fall", "fell", "drop", "dropped", "selloff",
    "sell-off", "plunge", "plunged",
]
SEVERE_TERMS = [
    "黑天鹅", "突发", "重大利空", "暴雷", "立案", "调查", "处罚", "禁令",
    "制裁", "出口管制", "断供", "违约", "退市", "停牌", "破产", "清盘",
    "召回", "事故", "爆炸", "火灾", "泄漏", "冲突", "战争", "封锁", "袭击",
    "限制", "整改", "下架", "禁售", "关税", "加征关税",
]

TECH_EXPOSURE_KEYWORDS = [
    "AI", "人工智能", "算力", "数据中心", "服务器", "GPU", "英伟达", "NVIDIA",
    "CPO", "光模块", "光通信", "PCB", "液冷", "云计算", "云服务", "半导体",
    "芯片", "存储", "先进封装", "封测", "光刻", "电子", "软件", "信创",
    "机器人", "科创", "科技", "计算机", "通信", "传媒",
]

RISK_THEMES: list[dict] = [
    {
        "id": "ai_compute_supply",
        "name": "AI算力/科技高估值",
        "sectors": ["AI算力", "半导体", "芯片", "通信", "电子", "计算机", "光模块", "CPO", "PCB"],
        "keywords": TECH_EXPOSURE_KEYWORDS,
        "trigger_terms": [
            "Meta", "META", "Facebook", "扎克伯格", "出租算力", "外租算力", "卖算力",
            "出售算力", "售卖算力", "租赁算力", "算力租赁", "剩余算力", "过剩算力",
            "闲置算力", "excess ai compute", "rent out", "raw computing capacity",
            "CoreWeave", "Nebius", "NVDA", "Nvidia", "英伟达", "博通", "美光",
        ],
        "negative_terms": [
            "供给过剩", "产能过剩", "需求放缓", "需求见顶", "需求天花板", "价格战",
            "泡沫", "过度建设", "资本开支下调", "削减资本开支", "砍单", "订单下修",
            "指引下修", "overbuilt", "oversupply", "capex cut", "spending cut",
        ] + SELL_OFF_TERMS,
        "action": "科技/AI算力方向先降仓，弱反弹不加仓，不做盘中抄底。",
    },
    {
        "id": "semiconductor_sanction",
        "name": "半导体出口管制/制裁",
        "sectors": ["半导体", "芯片", "电子", "设备材料", "先进封装", "EDA", "光刻机"],
        "keywords": ["半导体", "芯片", "光刻", "EDA", "先进封装", "HBM", "晶圆", "设备", "材料"],
        "trigger_terms": ["出口管制", "制裁", "实体清单", "禁令", "禁售", "BIS", "美商务部", "荷兰", "ASML", "光刻机"],
        "negative_terms": ["升级", "限制", "断供", "禁售", "调查", "处罚", "收紧", "扩大"] + SELL_OFF_TERMS,
        "action": "半导体链先防守，设备材料和高估值弹性票优先减暴露。",
    },
    {
        "id": "new_energy_auto",
        "name": "新能源车/锂电需求与召回",
        "sectors": ["新能源车", "汽车", "锂电池", "电力设备", "有色锂", "储能"],
        "keywords": ["新能源车", "电动车", "锂电", "电池", "储能", "光伏", "碳酸锂", "汽车", "特斯拉", "比亚迪", "宁德时代"],
        "trigger_terms": ["新能源车", "锂电", "电池", "储能", "光伏", "汽车", "特斯拉", "欧盟反补贴", "碳酸锂"],
        "negative_terms": ["召回", "起火", "爆炸", "需求放缓", "价格战", "补贴退坡", "关税", "反补贴", "砍单", "库存"] + SELL_OFF_TERMS,
        "action": "新能源链先看回撤，整车、锂电、储能、光伏中高弹性仓位冲高减。",
    },
    {
        "id": "medicine_policy",
        "name": "医药监管/临床/集采",
        "sectors": ["医药生物", "创新药", "医疗器械", "CXO", "疫苗"],
        "keywords": ["医药", "创新药", "CXO", "CRO", "医疗器械", "疫苗", "药品", "医保", "集采"],
        "trigger_terms": ["医药", "药品", "医疗器械", "疫苗", "医保谈判", "集采", "FDA", "临床"],
        "negative_terms": ["集采", "降价", "反腐", "临床失败", "暂停", "安全性", "调查", "处罚", "退市"] + SELL_OFF_TERMS,
        "action": "医药线先避开政策/临床冲击标的，集采和临床失败相关票优先降仓。",
    },
    {
        "id": "property_credit",
        "name": "地产信用/债务风险",
        "sectors": ["房地产", "建筑装饰", "建材", "银行", "信托"],
        "keywords": ["房地产", "地产", "房企", "物业", "建材", "建筑", "银行", "信托"],
        "trigger_terms": ["房地产", "地产", "房企", "债券", "信托", "按揭", "清盘", "重组"],
        "negative_terms": ["违约", "暴雷", "债务", "破产", "清盘", "停牌", "偿债", "烂尾", "流动性危机"] + SELL_OFF_TERMS,
        "action": "地产链和相关金融敞口先控仓，弱资质房企链条不接飞刀。",
    },
    {
        "id": "financial_credit",
        "name": "金融信用/监管风险",
        "sectors": ["银行", "非银金融", "证券", "保险", "信托"],
        "keywords": ["银行", "券商", "证券", "保险", "信托", "理财", "债券", "金融"],
        "trigger_terms": ["银行", "券商", "保险", "信托", "理财", "债券", "金融监管"],
        "negative_terms": ["挤兑", "坏账", "违约", "暴雷", "罚款", "监管", "资本充足", "亏损"] + SELL_OFF_TERMS,
        "action": "金融链先看信用传导，相关持仓降杠杆、等风险扩散确认后再说。",
    },
    {
        "id": "consumer_safety",
        "name": "消费/食品安全舆情",
        "sectors": ["食品饮料", "农林牧渔", "商贸零售", "餐饮", "白酒"],
        "keywords": ["食品", "白酒", "乳制品", "饮料", "预制菜", "餐饮", "消费", "农林牧渔"],
        "trigger_terms": ["食品", "白酒", "乳制品", "预制菜", "餐饮", "抽检", "消费"],
        "negative_terms": ["食品安全", "召回", "抽检不合格", "中毒", "致癌", "禁售", "舆情", "处罚"] + SELL_OFF_TERMS,
        "action": "消费食品安全相关票先规避舆情扩散，等公司澄清和资金承接。",
    },
    {
        "id": "media_game_policy",
        "name": "传媒游戏/教育监管",
        "sectors": ["传媒", "游戏", "教育", "互联网"],
        "keywords": ["游戏", "传媒", "影视", "短剧", "互联网", "教育", "教培"],
        "trigger_terms": ["游戏", "版号", "未成年人", "网游", "短剧", "教育", "教培"],
        "negative_terms": ["监管", "限制", "处罚", "下架", "禁令", "整改", "暂停"] + SELL_OFF_TERMS,
        "action": "传媒游戏教育线遇监管先降仓，题材反抽不追。",
    },
    {
        "id": "energy_chemical_accident",
        "name": "能源化工/矿山事故",
        "sectors": ["煤炭", "石油石化", "基础化工", "有色金属", "化工"],
        "keywords": ["煤炭", "煤矿", "石油", "石化", "化工", "有色", "矿山", "危化品"],
        "trigger_terms": ["煤矿", "化工", "石化", "油田", "炼化", "危化品", "矿山", "有色"],
        "negative_terms": ["爆炸", "火灾", "泄漏", "停产", "事故", "安全检查", "环保督察", "限产"] + SELL_OFF_TERMS,
        "action": "能源化工事故链先看停产和监管扩散，涉事区域/同业高风险票先减。",
    },
    {
        "id": "geopolitical_trade",
        "name": "地缘冲突/贸易链冲击",
        "sectors": ["航运港口", "物流", "外贸", "出口链", "军工", "石油石化", "贵金属"],
        "keywords": ["航运", "港口", "物流", "出口", "外贸", "军工", "石油", "黄金", "贵金属"],
        "trigger_terms": ["红海", "霍尔木兹", "台海", "中东", "俄乌", "关税", "贸易战", "制裁", "出口", "禁运"],
        "negative_terms": ["冲突", "封锁", "袭击", "升级", "禁运", "加征", "制裁", "断航"] + SELL_OFF_TERMS,
        "action": "地缘/贸易冲击下，出口链和高波动题材先控仓，避险线只看确认后的承接。",
    },
    {
        "id": "data_security",
        "name": "数据安全/网络安全事件",
        "sectors": ["计算机", "数据要素", "云服务", "网络安全", "互联网"],
        "keywords": ["数据安全", "网络安全", "云服务", "数据要素", "计算机", "软件", "互联网"],
        "trigger_terms": ["数据安全", "网络安全", "隐私", "个人信息", "黑客", "勒索", "数据泄露"],
        "negative_terms": ["处罚", "泄露", "攻击", "监管", "整改", "暂停", "下架"] + SELL_OFF_TERMS,
        "action": "数据安全事件相关软件/云服务先减风险，等影响边界清楚。",
    },
]

OPPORTUNITY_THEMES: list[dict] = [
    {
        "id": "policy_stimulus",
        "name": "稳增长/政策催化",
        "sectors": ["基建", "建筑装饰", "建材", "工程机械", "银行", "地产链", "消费"],
        "keywords": ["稳增长", "财政", "专项债", "基建", "地产", "消费", "以旧换新", "设备更新"],
        "trigger_terms": ["国常会", "发改委", "财政部", "专项债", "降准", "降息", "稳增长", "一揽子政策", "以旧换新", "设备更新"],
        "positive_terms": ["支持", "加码", "提振", "释放", "利好", "扩围", "补贴", "加快", "落地", "上调", "增加"],
        "action": "政策线先看资金承接，优先跟踪放量前排和低位首板，后排只做观察。",
    },
    {
        "id": "ai_capex_positive",
        "name": "AI算力/科技景气催化",
        "sectors": ["AI算力", "半导体", "通信", "光模块", "CPO", "PCB", "服务器", "液冷"],
        "keywords": TECH_EXPOSURE_KEYWORDS,
        "trigger_terms": ["AI", "人工智能", "算力", "GPU", "英伟达", "光模块", "服务器", "数据中心", "资本开支", "订单", "模型"],
        "positive_terms": ["订单", "资本开支增加", "上调指引", "需求旺盛", "供不应求", "突破", "涨价", "中标", "投资", "扩产", "创新高"],
        "action": "科技景气机会只看强承接前排，若同日风险雷达触发，则机会降级为观察。",
    },
    {
        "id": "semiconductor_localization",
        "name": "半导体国产替代",
        "sectors": ["半导体", "芯片", "设备材料", "先进封装", "光刻胶", "EDA"],
        "keywords": ["半导体", "芯片", "国产替代", "设备", "材料", "封装", "光刻胶", "EDA"],
        "trigger_terms": ["国产替代", "大基金", "半导体", "芯片", "设备", "材料", "封装", "光刻"],
        "positive_terms": ["突破", "量产", "订单", "中标", "扶持", "投资", "扩产", "验证通过", "放量"],
        "action": "半导体机会优先看设备材料和先进封装的放量确认，回避无量高开。",
    },
    {
        "id": "defense_geopolitical",
        "name": "军工/避险方向",
        "sectors": ["国防军工", "航天航空", "船舶", "卫星导航", "贵金属", "石油"],
        "keywords": ["军工", "航天", "航空", "船舶", "卫星", "黄金", "贵金属", "石油"],
        "trigger_terms": ["冲突", "战争", "军演", "地缘", "中东", "红海", "霍尔木兹", "制裁", "避险"],
        "positive_terms": ["升级", "避险", "上涨", "订单", "采购", "军贸", "油价上涨", "金价上涨"],
        "action": "地缘避险机会先看军工、黄金、油气的承接，情绪化高开不追。",
    },
    {
        "id": "resource_price",
        "name": "资源品涨价",
        "sectors": ["有色金属", "贵金属", "煤炭", "石油石化", "基础化工", "锂矿", "稀土"],
        "keywords": ["铜", "铝", "黄金", "白银", "煤炭", "石油", "化工", "锂", "稀土", "钨", "锗"],
        "trigger_terms": ["涨价", "价格上涨", "库存下降", "供应收缩", "限产", "减产", "停产", "需求回升"],
        "positive_terms": ["涨价", "上涨", "供不应求", "库存下降", "减产", "限产", "利润改善", "创新高"],
        "action": "资源品机会看价格持续性和期货联动，优先跟踪龙头和有业绩弹性的标的。",
    },
    {
        "id": "consumer_recovery",
        "name": "消费复苏/补贴",
        "sectors": ["食品饮料", "家电", "汽车", "商贸零售", "旅游酒店", "免税"],
        "keywords": ["消费", "家电", "汽车", "白酒", "旅游", "酒店", "免税", "零售"],
        "trigger_terms": ["消费", "以旧换新", "补贴", "旅游", "免税", "白酒", "家电", "汽车"],
        "positive_terms": ["复苏", "回暖", "增长", "补贴", "旺季", "提价", "销量增长", "政策支持"],
        "action": "消费机会优先看政策受益和业绩兑现，弱趋势标的只做观察。",
    },
    {
        "id": "medicine_innovation",
        "name": "创新药/医疗突破",
        "sectors": ["创新药", "医药生物", "CXO", "医疗器械", "疫苗"],
        "keywords": ["创新药", "医药", "CXO", "CRO", "医疗器械", "疫苗", "临床", "FDA"],
        "trigger_terms": ["创新药", "临床", "获批", "FDA", "医保", "出海", "授权", "BD"],
        "positive_terms": ["获批", "突破", "授权", "出海", "临床成功", "纳入医保", "订单", "合作"],
        "action": "医药机会先看临床/出海兑现，避开同日监管或集采利空方向。",
    },
    {
        "id": "robotics_embodied_ai",
        "name": "机器人/具身智能",
        "sectors": ["机器人", "机械设备", "减速器", "电机", "传感器", "自动化"],
        "keywords": ["机器人", "具身智能", "减速器", "丝杠", "电机", "传感器", "自动化"],
        "trigger_terms": ["机器人", "具身智能", "人形机器人", "量产", "订单", "发布会"],
        "positive_terms": ["量产", "订单", "发布", "突破", "合作", "供应商", "投资", "增长"],
        "action": "机器人机会看核心零部件和量产订单，主题高位时只看分歧承接。",
    },
]


def _as_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_as_text(item) for item in value)
    if isinstance(value, dict):
        return " ".join(_as_text(v) for v in value.values())
    return str(value)


def _row_text(row: dict) -> str:
    return " ".join(
        _as_text(row.get(key))
        for key in ("title", "content", "summary", "subjects", "stocks", "source")
    )


def _contains(text: str, terms: Iterable[str]) -> bool:
    lower = text.lower()
    return any(str(term).lower() in lower for term in terms)


def _parse_jsonish(value):
    if isinstance(value, (list, dict)):
        return value
    if not value:
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _safe_date_text(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")


def _news_stock_refs(row: dict) -> list[dict]:
    stocks = _parse_jsonish(row.get("stocks")) or []
    if not isinstance(stocks, list):
        return []
    refs = []
    for item in stocks:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or item.get("stock_code") or "").strip()
        if code:
            code = re.sub(r"[^0-9]", "", code)[-6:].zfill(6)
        name = str(item.get("name") or item.get("stock_name") or "").strip()
        if code or name:
            refs.append({"code": code, "name": name})
    return refs


def _news_subject_names(row: dict) -> list[str]:
    subjects = _parse_jsonish(row.get("subjects")) or []
    if not isinstance(subjects, list):
        return []
    names = []
    for item in subjects:
        name = item.get("name") if isinstance(item, dict) else str(item)
        if name:
            names.append(str(name))
    return names


def _direct_stock_hit(row_text: str, holding: dict, stock_refs: list[dict]) -> bool:
    code = str(holding.get("stock_code") or "").strip().zfill(6)
    name = str(holding.get("short_name") or holding.get("display_name") or holding.get("stock_name") or "").strip()
    for ref in stock_refs:
        if code and ref.get("code") == code:
            return True
        if name and ref.get("name") and ref["name"] in name:
            return True
        if name and ref.get("name") and ref["name"] in row_text:
            return True
    return bool(name and name in row_text)


def _theme_matches_news(theme: dict, text: str) -> tuple[bool, list[str], int]:
    labels: list[str] = []
    score = 0
    has_trigger = _contains(text, theme["trigger_terms"]) or _contains(text, theme["keywords"])
    has_negative = _contains(text, theme["negative_terms"])
    has_severe = _contains(text, SEVERE_TERMS)
    has_selloff = _contains(text, SELL_OFF_TERMS)
    if has_trigger and (has_negative or has_severe or has_selloff):
        labels.append(theme["name"])
        score += 42
    if has_trigger and has_severe:
        labels.append("黑天鹅/重大风险词")
        score += 22
    if has_trigger and has_selloff:
        labels.append("市场价格已出现负反馈")
        score += 18
    if has_trigger and has_negative:
        labels.append("主题负面触发词")
        score += 14
    return bool(labels), labels, min(score, 100)


def _match_news_black_swan(row: dict) -> list[dict]:
    row = {
        **row,
        "subjects": _parse_jsonish(row.get("subjects")),
        "stocks": _parse_jsonish(row.get("stocks")),
    }
    text = _row_text(row)
    if not text.strip():
        return []

    stock_refs = _news_stock_refs(row)
    subject_names = _news_subject_names(row)
    title = re.sub(r"\s+", " ", str(row.get("title") or row.get("content") or "").strip())[:180]
    hits: list[dict] = []
    for theme in RISK_THEMES:
        matched, labels, score = _theme_matches_news(theme, text)
        if not matched:
            continue
        if stock_refs:
            score += 10
        hits.append({
            "theme_id": theme["id"],
            "theme": theme["name"],
            "title": title,
            "source": str(row.get("source") or ""),
            "publish_time": _safe_date_text(row.get("publish_time") or row.get("time")),
            "labels": labels,
            "score": min(score, 100),
            "affected_sectors": theme["sectors"][:],
            "stock_refs": stock_refs,
            "subjects": subject_names,
        })

    if not hits and _contains(text, SEVERE_TERMS) and (stock_refs or subject_names):
        hits.append({
            "theme_id": "unclassified",
            "theme": "未分类黑天鹅",
            "title": title,
            "source": str(row.get("source") or ""),
            "publish_time": _safe_date_text(row.get("publish_time") or row.get("time")),
            "labels": ["重大风险词", "需人工复核"],
            "score": 55 + (10 if stock_refs else 0),
            "affected_sectors": subject_names[:6],
            "stock_refs": stock_refs,
            "subjects": subject_names,
        })
    return hits


def _match_news_opportunity(row: dict) -> list[dict]:
    row = {
        **row,
        "subjects": _parse_jsonish(row.get("subjects")),
        "stocks": _parse_jsonish(row.get("stocks")),
    }
    text = _row_text(row)
    if not text.strip():
        return []

    stock_refs = _news_stock_refs(row)
    subject_names = _news_subject_names(row)
    title = re.sub(r"\s+", " ", str(row.get("title") or row.get("content") or "").strip())[:180]
    hits: list[dict] = []
    for theme in OPPORTUNITY_THEMES:
        has_trigger = _contains(text, theme["trigger_terms"]) or _contains(text, theme["keywords"])
        has_positive = _contains(text, theme["positive_terms"])
        if not (has_trigger and has_positive):
            continue
        score = 44 + (16 if stock_refs else 0)
        if any(term in text for term in ("涨停", "大涨", "突破", "中标", "获批", "订单", "上调")):
            score += 12
        hits.append({
            "theme_id": theme["id"],
            "theme": theme["name"],
            "title": title,
            "source": str(row.get("source") or ""),
            "publish_time": _safe_date_text(row.get("publish_time") or row.get("time")),
            "labels": ["机会催化", theme["name"]],
            "score": min(score, 100),
            "sectors": theme["sectors"][:],
            "stock_refs": stock_refs,
            "subjects": subject_names,
            "action": theme["action"],
        })
    return hits


def _holding_text(row: dict) -> str:
    return " ".join(
        _as_text(row.get(key))
        for key in (
            "industry_name", "industry", "sector", "concept_tag", "pop_tag",
            "concepts", "notes", "short_name", "display_name", "stock_name",
        )
    )


def is_tech_exposed(row: dict) -> bool:
    """Legacy helper kept for existing imports."""
    return _contains(_holding_text(row), TECH_EXPOSURE_KEYWORDS)


def _holding_matches_theme(row: dict, hit: dict) -> tuple[bool, str]:
    text = _holding_text(row)
    row_text = _as_text(row)
    if _direct_stock_hit(row_text + " " + text, row, hit.get("stock_refs") or []):
        return True, "新闻直接点名"
    affected = hit.get("affected_sectors") or []
    if affected and _contains(text, affected):
        return True, "命中受冲击板块"
    theme = next((item for item in RISK_THEMES if item["id"] == hit.get("theme_id")), None)
    if theme and _contains(text, theme["keywords"]):
        return True, "命中主题暴露"
    return False, ""


def _candidate_matches_theme(row: dict, hit: dict) -> tuple[bool, str]:
    text = _holding_text(row)
    row_text = _as_text(row)
    if _direct_stock_hit(row_text + " " + text, row, hit.get("stock_refs") or []):
        return True, "新闻直接点名"
    sectors = hit.get("sectors") or hit.get("affected_sectors") or []
    if sectors and _contains(text, sectors):
        return True, "命中机会板块"
    theme = next((item for item in OPPORTUNITY_THEMES if item["id"] == hit.get("theme_id")), None)
    if theme and _contains(text, theme["keywords"]):
        return True, "命中主题暴露"
    return False, ""


def _holding_brief(row: dict, matched_hits: list[dict] | None = None) -> dict:
    shares = int(float(row.get("shares") or 0))
    current_price = row.get("cur_price")
    if current_price is None:
        current_price = row.get("price") or row.get("close")
    profit_pct = row.get("profit_pct")
    if profit_pct is None and current_price not in (None, "") and row.get("cost_price"):
        try:
            cost = float(row.get("cost_price") or 0)
            cur = float(current_price or 0)
            if cost > 0 and cur > 0:
                profit_pct = round((cur / cost - 1) * 100, 2)
        except Exception:
            profit_pct = None
    themes = []
    reasons = []
    for hit in matched_hits or []:
        if hit.get("theme") and hit["theme"] not in themes:
            themes.append(hit["theme"])
        if hit.get("match_reason") and hit["match_reason"] not in reasons:
            reasons.append(hit["match_reason"])
    return {
        "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
        "short_name": row.get("display_name") or row.get("short_name") or row.get("stock_name") or "",
        "shares": shares,
        "industry_name": row.get("industry_name") or row.get("industry") or "",
        "concept_tag": row.get("concept_tag") or row.get("pop_tag") or "",
        "change_pct": row.get("change_pct"),
        "profit_pct": profit_pct,
        "cur_price": current_price,
        "themes": themes[:4],
        "match_reasons": reasons[:4],
    }


def _aggregate_sector_risks(matches: list[dict], sector_rows: list[dict]) -> list[dict]:
    sector_map: dict[str, dict] = {}
    for hit in matches:
        for sector in hit.get("affected_sectors") or []:
            if not sector:
                continue
            item = sector_map.setdefault(str(sector), {"name": str(sector), "score": 0, "themes": set(), "news_count": 0})
            item["score"] = max(item["score"], float(hit.get("score") or 0))
            item["themes"].add(str(hit.get("theme") or ""))
            item["news_count"] += 1

    for row in (sector_rows or [])[:100]:
        name = str(row.get("concept_name") or row.get("name") or row.get("industry_name") or "")
        if not name:
            continue
        try:
            chg = float(row.get("change_pct") if row.get("change_pct") is not None else row.get("change") or 0)
        except Exception:
            chg = 0.0
        for sector_name, item in sector_map.items():
            if sector_name in name or name in sector_name:
                item["change_pct"] = round(chg, 2)
                if chg <= -2:
                    item["score"] = min(100, item["score"] + 10)

    out = []
    for item in sector_map.values():
        out.append({
            "name": item["name"],
            "score": round(float(item["score"]), 1),
            "themes": [t for t in item["themes"] if t][:4],
            "news_count": int(item["news_count"]),
            "change_pct": item.get("change_pct"),
        })
    out.sort(key=lambda item: (-float(item["score"]), -int(item["news_count"])))
    return out[:10]


def _num(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _market_context(market_rows: list[dict] | None, sector_rows: list[dict] | None) -> dict:
    rows = market_rows or []
    overview = next((row for row in rows if row.get("_kind") == "overview"), {})
    indices = [row for row in rows if row.get("_kind") == "index"]
    up_count = int(_num(overview.get("up_count")))
    down_count = int(_num(overview.get("down_count")))
    total = int(_num(overview.get("total"))) or up_count + down_count
    red_ratio = round(up_count / total * 100, 1) if total > 0 else None
    avg_change = _num(overview.get("avg_change_pct"), 0)
    market_heat = red_ratio if red_ratio is not None else 50
    risk_bias = 0.0
    opportunity_bias = 0.0
    evidence: list[str] = []

    if red_ratio is not None:
        if red_ratio < 35:
            risk_bias += 22
            evidence.append(f"A股红盘率{red_ratio:.1f}%偏弱")
        elif red_ratio > 60:
            opportunity_bias += 16
            evidence.append(f"A股红盘率{red_ratio:.1f}%偏强")
    if avg_change <= -1.2:
        risk_bias += 16
        evidence.append(f"全市场均涨跌{avg_change:+.2f}%")
    elif avg_change >= 0.8:
        opportunity_bias += 10
        evidence.append(f"全市场均涨跌{avg_change:+.2f}%")

    tech_index = [idx for idx in indices if str(idx.get("index_code")) in {"399006", "000688"}]
    if tech_index and all(_num(idx.get("change_pct")) <= -1.5 for idx in tech_index):
        risk_bias += 12
        evidence.append("创业板/科创50同步走弱")
    if tech_index and any(_num(idx.get("change_pct")) >= 1.5 for idx in tech_index):
        opportunity_bias += 8
        evidence.append("成长指数出现正反馈")

    strong = []
    weak = []
    for row in (sector_rows or [])[:120]:
        name = str(row.get("concept_name") or row.get("name") or row.get("industry_name") or "")
        chg = _num(row.get("change_pct") if row.get("change_pct") is not None else row.get("change"))
        if chg >= 2:
            strong.append({"name": name, "change_pct": round(chg, 2)})
        if chg <= -2:
            weak.append({"name": name, "change_pct": round(chg, 2)})
    if len(weak) >= 4:
        risk_bias += min(16, len(weak) * 3)
        evidence.append(f"弱势板块{len(weak)}个")
    if len(strong) >= 4:
        opportunity_bias += min(16, len(strong) * 3)
        evidence.append(f"强势板块{len(strong)}个")

    phase = "risk_off" if risk_bias >= opportunity_bias + 12 else "risk_on" if opportunity_bias >= risk_bias + 12 else "mixed"
    return {
        "phase": phase,
        "market_heat": round(market_heat, 1) if market_heat is not None else None,
        "red_ratio": red_ratio,
        "avg_change_pct": round(avg_change, 2),
        "risk_bias": round(risk_bias, 1),
        "opportunity_bias": round(opportunity_bias, 1),
        "strong_sectors": strong[:10],
        "weak_sectors": weak[:10],
        "indices": indices[:8],
        "evidence": evidence[:10],
    }


def _candidate_brief(row: dict, matched_hits: list[dict] | None = None) -> dict:
    themes = []
    reasons = []
    for hit in matched_hits or []:
        if hit.get("theme") and hit["theme"] not in themes:
            themes.append(hit["theme"])
        if hit.get("match_reason") and hit["match_reason"] not in reasons:
            reasons.append(hit["match_reason"])
    score = max(
        _num(row.get("final_trade_score")),
        _num(row.get("ai_score")),
        _num(row.get("total_score")),
        100 - _num(row.get("fused_rank"), 100),
    )
    return {
        "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
        "short_name": row.get("short_name") or row.get("stock_name") or "",
        "score": round(score, 1),
        "source": row.get("_source") or row.get("source") or "",
        "signal_status": row.get("signal_status") or row.get("recommend_status") or "",
        "industry_name": row.get("industry_name") or row.get("industry") or "",
        "concept_tag": row.get("concept_tag") or row.get("pop_tag") or "",
        "change_pct": row.get("change_pct"),
        "themes": themes[:3],
        "match_reasons": reasons[:3],
    }


def _build_opportunity_signal(
    news_rows: list[dict],
    sector_rows: list[dict],
    candidate_rows: list[dict],
    portfolio_rows: list[dict],
    market: dict,
    risk_signal: dict,
) -> dict:
    matches: list[dict] = []
    for row in news_rows[:160]:
        matches.extend(_match_news_opportunity(row))
    matches.sort(key=lambda item: float(item.get("score") or 0), reverse=True)

    sector_scores: dict[str, dict] = {}
    for hit in matches:
        for sector in hit.get("sectors") or []:
            item = sector_scores.setdefault(sector, {"name": sector, "score": 0.0, "themes": set(), "news_count": 0})
            item["score"] = max(item["score"], float(hit.get("score") or 0))
            item["themes"].add(hit.get("theme") or "")
            item["news_count"] += 1

    for row in sector_rows[:120]:
        name = str(row.get("concept_name") or row.get("name") or row.get("industry_name") or "")
        chg = _num(row.get("change_pct") if row.get("change_pct") is not None else row.get("change"))
        if chg < 1.5:
            continue
        item = sector_scores.setdefault(name, {"name": name, "score": 35.0, "themes": set(), "news_count": 0})
        item["score"] = min(100, max(item["score"], 38 + min(28, chg * 5) + _num(row.get("hot_value")) / 8))
        item["change_pct"] = round(chg, 2)
        item["themes"].add("A股板块强势确认")

    opportunity_sectors = []
    for item in sector_scores.values():
        opportunity_sectors.append({
            "name": item["name"],
            "score": round(float(item["score"]), 1),
            "themes": [t for t in item["themes"] if t][:4],
            "news_count": int(item["news_count"]),
            "change_pct": item.get("change_pct"),
        })
    opportunity_sectors.sort(key=lambda item: (-float(item["score"]), -int(item["news_count"])))

    candidate_hits: list[dict] = []
    seen_codes: set[str] = set()
    for row in list(candidate_rows or []) + list(portfolio_rows or []):
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if not code or code in seen_codes:
            continue
        matched_hits = []
        for hit in matches[:20]:
            ok, reason = _candidate_matches_theme(row, hit)
            if ok:
                matched_hits.append({**hit, "match_reason": reason})
        if not matched_hits:
            row_text = _holding_text(row)
            for sector in opportunity_sectors[:10]:
                if sector.get("name") and sector["name"] in row_text:
                    matched_hits.append({
                        "theme": "A股板块强势确认",
                        "theme_id": "market_strength",
                        "match_reason": "命中强势板块",
                    })
                    break
        if matched_hits:
            seen_codes.add(code)
            candidate_hits.append(_candidate_brief(row, matched_hits))
    candidate_hits.sort(key=lambda item: -float(item.get("score") or 0))

    max_news = max([_num(item.get("score")) for item in matches] or [0])
    max_sector = max([_num(item.get("score")) for item in opportunity_sectors] or [0])
    score = min(100, max(max_news, max_sector) + _num(market.get("opportunity_bias")) - _num(market.get("risk_bias")) * 0.45)

    if score >= 72:
        status = "focus"
        headline = "机会方向有新闻和盘面共振"
    elif score >= 52:
        status = "watch"
        headline = "机会方向出现观察信号"
    else:
        status = "clear"
        headline = "暂无高确定性机会信号"

    sectors_text = "、".join(item["name"] for item in opportunity_sectors[:5]) or "强势方向"
    if status == "focus":
        action = f"{sectors_text}可列入今日重点观察，只看前排放量和回踩承接。"
    elif status == "watch":
        action = f"{sectors_text}有苗头，等资金和指数确认后再动手。"
    else:
        action = "机会侧暂不强行找方向，先等板块和资金共振。"

    return {
        "status": status,
        "triggered": status == "focus",
        "score": round(max(0, score), 1),
        "headline": headline,
        "summary": headline,
        "action": action,
        "news_hits": matches[:8],
        "opportunity_sectors": opportunity_sectors[:10],
        "candidate_stocks": candidate_hits[:12],
        "candidate_count": len(candidate_hits),
    }


def build_black_swan_signal(
    news_rows: list[dict] | None,
    portfolio_rows: list[dict] | None = None,
    sector_rows: list[dict] | None = None,
    market_rows: list[dict] | None = None,
    candidate_rows: list[dict] | None = None,
) -> dict:
    """Build a structured black-swan signal for sectors and actual holdings."""
    news_rows = news_rows or []
    portfolio_rows = portfolio_rows or []
    sector_rows = sector_rows or []
    market = _market_context(market_rows, sector_rows)

    matches: list[dict] = []
    for row in news_rows[:160]:
        matches.extend(_match_news_black_swan(row))

    matches.sort(key=lambda item: float(item.get("score") or 0), reverse=True)
    sector_risks = _aggregate_sector_risks(matches, sector_rows)

    exposed_holdings = []
    for row in portfolio_rows:
        if int(float(row.get("shares") or 0)) <= 0:
            continue
        matched_hits = []
        for hit in matches[:20]:
            ok, reason = _holding_matches_theme(row, hit)
            if ok:
                matched_hits.append({**hit, "match_reason": reason})
        if matched_hits:
            exposed_holdings.append(_holding_brief(row, matched_hits))

    max_news_score = max([float(item.get("score") or 0) for item in matches] or [0])
    max_sector_score = max([float(item.get("score") or 0) for item in sector_risks] or [0])
    direct_holding_bonus = 18 if exposed_holdings else 0
    score = min(
        100,
        max(max_news_score, max_sector_score)
        + min(18, len(matches) * 3)
        + direct_holding_bonus
        + _num(market.get("risk_bias")) * 0.8,
    )

    if score >= 78:
        status = "escape_now"
        headline = "黑天鹅板块触发先跑预警"
    elif score >= 55:
        status = "reduce"
        headline = "黑天鹅板块触发减仓防守"
    elif score >= 35:
        status = "watch"
        headline = "黑天鹅板块出现分歧信号"
    else:
        status = "clear"
        headline = "暂无黑天鹅板块触发"
    triggered = status in {"escape_now", "reduce"}

    top_sectors = "、".join(item["name"] for item in sector_risks[:5]) or "相关板块"
    if triggered:
        action = f"今天{top_sectors}先防守：命中板块不加仓，命中个股冲高先减，弱反弹不抄底。"
    elif status == "watch":
        action = f"{top_sectors}出现风险苗头，先观察新闻扩散和资金承接。"
    else:
        action = "未检测到需要单独跑的黑天鹅板块触发器。"
    if triggered and exposed_holdings:
        names = "、".join(f"{item['short_name']}({item['stock_code']})" for item in exposed_holdings[:8])
        action += f" 命中实际持仓：{names}。"

    evidence_labels: list[str] = []
    for item in matches:
        for label in item.get("labels") or []:
            if label and label not in evidence_labels:
                evidence_labels.append(label)

    risk_payload = {
        "status": status,
        "triggered": triggered,
        "score": round(score, 1),
        "headline": headline,
        "summary": headline if triggered else "暂未触发黑天鹅先跑信号",
        "action": action,
        "news_hits": matches[:8],
        "news_hit_count": len(matches),
        "evidence": evidence_labels[:10],
        "affected_sectors": sector_risks,
        "weak_tech_sectors": [item for item in sector_risks if any(k in item["name"] for k in TECH_EXPOSURE_KEYWORDS)],
        "exposed_holdings": exposed_holdings,
        "exposed_holding_count": len(exposed_holdings),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    opportunity_payload = _build_opportunity_signal(
        news_rows,
        sector_rows,
        candidate_rows or [],
        portfolio_rows,
        market,
        risk_payload,
    )
    return {
        **risk_payload,
        "risk": risk_payload,
        "opportunity": opportunity_payload,
        "market_context": market,
    }


def holding_matches_signal(row: dict, signal: dict | None) -> bool:
    if not signal or not signal.get("triggered"):
        return False
    code = str(row.get("stock_code") or "").strip().zfill(6)
    name = str(row.get("short_name") or row.get("display_name") or row.get("stock_name") or "")
    for item in signal.get("exposed_holdings") or []:
        if code and str(item.get("stock_code") or "").zfill(6) == code:
            return True
        if name and item.get("short_name") and str(item["short_name"]) in name:
            return True
    for hit in signal.get("news_hits") or []:
        ok, _ = _holding_matches_theme(row, hit)
        if ok:
            return True
    return False


def _load_recent_news(query: QueryFn, trade_date: str | None = None, days: int = 2) -> list[dict]:
    days = max(1, min(int(days or 2), 7))
    select_cols = "source, title, content, publish_time, subjects, stocks"
    try:
        if trade_date:
            return query(
                f"""
                SELECT {select_cols}
                FROM st_news_flash
                WHERE DATE(publish_time) >= DATE_SUB(:trade_date, INTERVAL {days} DAY)
                  AND DATE(publish_time) <= DATE_ADD(:trade_date, INTERVAL 1 DAY)
                ORDER BY is_top DESC, publish_time DESC
                LIMIT 180
                """,
                {"trade_date": str(trade_date)[:10]},
            )
        return query(
            f"""
            SELECT {select_cols}
            FROM st_news_flash
            WHERE publish_time >= DATE_SUB(NOW(), INTERVAL {days} DAY)
            ORDER BY is_top DESC, publish_time DESC
            LIMIT 180
            """,
            {},
        )
    except Exception:
        return []


def _load_portfolio(query: QueryFn) -> list[dict]:
    try:
        return query(
            """
            SELECT p.stock_code,
                   COALESCE(NULLIF(p.short_name, ''), p.stock_code) AS short_name,
                   p.shares, p.cost_price, p.notes,
                   s.industry AS industry_name,
                   s.close AS cur_price,
                   s.change_pct,
                   t.concept_tag,
                   t.pop_tag
            FROM st_user_portfolio p
            LEFT JOIN sm_stock_snapshot s ON s.stock_code = p.stock_code
            LEFT JOIN st_hot_rank_ths t
              ON t.stock_code = p.stock_code COLLATE utf8mb4_unicode_ci
             AND t.snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_ths)
            WHERE p.shares > 0
            ORDER BY p.sort_order, p.id
            """,
            {},
        )
    except Exception:
        try:
            return query(
                """
                SELECT stock_code, short_name, shares, cost_price, notes
                FROM st_user_portfolio
                WHERE shares > 0
                ORDER BY sort_order, id
                """,
                {},
            )
        except Exception:
            return []


def _load_sector_rows(query: QueryFn, trade_date: str | None = None) -> list[dict]:
    try:
        params = {"trade_date": str(trade_date)[:10]} if trade_date else {}
        where = (
            "snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_concept_ths_daily WHERE snapshot_date <= :trade_date)"
            if trade_date else
            "snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_concept_ths_daily)"
        )
        return query(
            f"""
            SELECT concept_name, change_pct, hot_value, plate_type
            FROM st_hot_concept_ths_daily
            WHERE {where}
            ORDER BY plate_type, `rank`
            LIMIT 120
            """,
            params,
        )
    except Exception:
        return []


def _load_market_rows(query: QueryFn, trade_date: str | None = None) -> list[dict]:
    rows: list[dict] = []
    try:
        params = {"trade_date": str(trade_date)[:10]} if trade_date else {}
        date_clause = "AND trade_date <= :trade_date" if trade_date else ""
        overview = query(
            f"""
            SELECT 'overview' AS _kind, trade_date,
                   COUNT(*) AS total,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_count,
                   SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_count,
                   AVG(change_pct) AS avg_change_pct,
                   COALESCE(SUM(amount), 0) AS amount
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date = (
                  SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1 {date_clause}
              )
            GROUP BY trade_date
            """,
            params,
        )
        rows.extend(overview)
    except Exception as exc:
        logger.debug("market overview lookup failed while loading risk context: %s", exc)
    try:
        rows.extend(query(
            """
            SELECT 'index' AS _kind, index_code, price, change_pct
            FROM sm_index_current
            WHERE index_code IN ('000001','399001','399006','000688','000300','000852')
            """,
            {},
        ))
    except Exception as exc:
        logger.debug("index current lookup failed while loading risk context: %s", exc)
    return rows


def _load_candidate_rows(query: QueryFn, trade_date: str | None = None) -> list[dict]:
    candidates: list[dict] = []
    try:
        params = {"trade_date": str(trade_date)[:10]} if trade_date else {}
        date_filter = (
            "pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks WHERE pick_date <= :trade_date)"
            if trade_date else
            "pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks)"
        )
        candidates.extend(query(
            f"""
            SELECT 'recommended' AS _source,
                   r.stock_code, r.short_name, r.final_trade_score, r.ai_score,
                   r.signal_status, r.recommend_status, r.primary_strategy,
                   s.industry AS industry_name, t.concept_tag, t.pop_tag,
                   COALESCE(r.change_pct, s.change_pct) AS change_pct
            FROM st_recommended_stocks r
            LEFT JOIN sm_stock_snapshot s ON s.stock_code = r.stock_code
            LEFT JOIN st_hot_rank_ths t
              ON t.stock_code = r.stock_code COLLATE utf8mb4_unicode_ci
             AND t.snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_ths)
            WHERE {date_filter}
            ORDER BY COALESCE(r.final_trade_score, r.ai_score, 0) DESC
            LIMIT 80
            """,
            params,
        ))
    except Exception:
        try:
            candidates.extend(query(
                """
                SELECT 'recommended' AS _source, stock_code, short_name, ai_score,
                       recommend_status, signal_status
                FROM st_recommended_stocks
                WHERE pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks)
                ORDER BY ai_score DESC
                LIMIT 80
                """,
                {},
            ))
        except Exception as exc:
            logger.debug("recommended-stock candidate lookup failed: %s", exc)
    try:
        params = {"trade_date": str(trade_date)[:10]} if trade_date else {}
        date_filter = (
            "f.snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_fused WHERE snapshot_date <= :trade_date)"
            if trade_date else
            "f.snapshot_date = (SELECT MAX(snapshot_date) FROM st_hot_rank_fused)"
        )
        candidates.extend(query(
            f"""
            SELECT 'hot_rank' AS _source,
                   f.stock_code, f.short_name, f.fused_rank,
                   f.total_score, f.change_pct, f.industry_name,
                   t.concept_tag, t.pop_tag
            FROM st_hot_rank_fused f
            LEFT JOIN st_hot_rank_ths t
              ON t.stock_code = f.stock_code COLLATE utf8mb4_unicode_ci
             AND t.snapshot_date = f.snapshot_date
            WHERE {date_filter}
            ORDER BY f.fused_rank
            LIMIT 100
            """,
            params,
        ))
    except Exception as exc:
        logger.debug("hot-rank candidate lookup failed: %s", exc)
    return candidates


def fetch_black_swan_signal(
    query: QueryFn,
    trade_date: str | None = None,
    *,
    days: int = 2,
    news_rows: list[dict] | None = None,
    portfolio_rows: list[dict] | None = None,
    sector_rows: list[dict] | None = None,
    market_rows: list[dict] | None = None,
    candidate_rows: list[dict] | None = None,
) -> dict:
    news = news_rows if news_rows is not None else _load_recent_news(query, trade_date, days)
    portfolio = portfolio_rows if portfolio_rows is not None else _load_portfolio(query)
    sectors = sector_rows if sector_rows is not None else _load_sector_rows(query, trade_date)
    market = market_rows if market_rows is not None else _load_market_rows(query, trade_date)
    candidates = candidate_rows if candidate_rows is not None else _load_candidate_rows(query, trade_date)
    return build_black_swan_signal(news, portfolio, sectors, market, candidates)


def format_black_swan_markdown(signal: dict | None, title: str = "⚠️ 风险/机会决策雷达") -> str:
    opportunity = signal.get("opportunity") if signal else {}
    opportunity_active = (opportunity or {}).get("status") in {"focus", "watch"} or bool((opportunity or {}).get("triggered"))
    if not signal or (not signal.get("triggered") and not opportunity_active):
        return ""

    lines = [f"### {title}"]
    lines.append(f'> <font color="warning">风险：{signal.get("headline")}</font>')
    lines.append(f"> 防守动作：{signal.get('action')}")
    market = signal.get("market_context") or {}
    if market.get("evidence"):
        lines.append(f"> 市场确认：{'；'.join(market['evidence'][:4])}")

    sectors = signal.get("affected_sectors") or []
    if sectors:
        lines.append("")
        lines.append("**受冲击板块**：" + "、".join(item.get("name", "") for item in sectors[:8] if item.get("name")))

    holdings = signal.get("exposed_holdings") or []
    if holdings:
        lines.append("")
        lines.append("**命中实际持仓**")
        for item in holdings[:10]:
            extras = []
            if item.get("themes"):
                extras.append("主题 " + "、".join(item["themes"][:2]))
            if item.get("match_reasons"):
                extras.append("原因 " + "、".join(item["match_reasons"][:2]))
            if item.get("profit_pct") is not None:
                extras.append(f"持仓盈亏 {float(item['profit_pct']):+.2f}%")
            if item.get("change_pct") is not None:
                extras.append(f"涨跌 {float(item['change_pct']):+.2f}%")
            lines.append(f"- {item.get('short_name') or item.get('stock_code')}({item.get('stock_code')})：{'；'.join(extras) or '命中黑天鹅暴露'}")

    hits = signal.get("news_hits") or []
    if hits:
        lines.append("")
        lines.append("**新闻证据**")
        for item in hits[:4]:
            meta = " / ".join([v for v in [item.get("source"), item.get("publish_time")] if v])
            sectors_text = "、".join((item.get("affected_sectors") or [])[:4])
            suffix = f"；板块：{sectors_text}" if sectors_text else ""
            lines.append(f"- {item.get('title') or '相关快讯'}" + (f"（{meta}）" if meta else "") + suffix)

    opportunity = signal.get("opportunity") or {}
    if opportunity and opportunity.get("status") != "clear":
        lines.append("")
        lines.append(f'**机会侧**：{opportunity.get("headline")}；{opportunity.get("action")}')
        sectors2 = opportunity.get("opportunity_sectors") or []
        if sectors2:
            lines.append("- 机会板块：" + "、".join(item.get("name", "") for item in sectors2[:8] if item.get("name")))
        stocks = opportunity.get("candidate_stocks") or []
        if stocks:
            stock_text = "、".join(
                f"{item.get('short_name') or item.get('stock_code')}({item.get('stock_code')})"
                for item in stocks[:8]
            )
            lines.append("- 观察个股：" + stock_text)

    lines.append("")
    lines.append("> 仅作决策提醒：先确认风险是否命中持仓，再看机会是否有资金和板块共振；具体买卖仍结合盘口流动性、个股位置和账户纪律执行。")
    return "\n".join(lines)


def append_black_swan_markdown(content: str, signal: dict | None) -> str:
    section = format_black_swan_markdown(signal)
    if not section or "风险/机会决策雷达" in (content or "") or "黑天鹅板块风险雷达" in (content or ""):
        return content
    return (content or "").rstrip() + "\n\n" + section


# Backward-compatible names used by the first integration pass.
build_tech_risk_signal = build_black_swan_signal
fetch_tech_risk_signal = fetch_black_swan_signal
format_tech_risk_markdown = format_black_swan_markdown
append_tech_risk_markdown = append_black_swan_markdown
