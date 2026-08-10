#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Research radar for blogger/official-account/rapport trend tracking.

The module is intentionally read-only: it combines a curated source map with
local market tables when available, so the website, morning briefing, and
evening review can all show the same research context.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any

from sqlalchemy import text
from server.common.kline_data import get_kline_engine, should_use_kline_engine


SOURCE_SIGNALS: list[dict[str, Any]] = [
    {
        "name": "红旗大街发哥",
        "type": "抖音博主",
        "focus": "龙虎榜、短线复盘、资金情绪",
        "weight": "情绪层",
        "usage": "用于观察短线资金偏好，不直接作为基本面依据",
    },
    {
        "name": "全能的野人",
        "type": "抖音博主",
        "focus": "大盘情绪、机器人、储能电池、个股热度",
        "weight": "题材层",
        "usage": "用于发现近期反复被市场讨论的产业主题",
    },
    {
        "name": "滚雪球的猫菲特",
        "type": "抖音/微信公众号",
        "focus": "成长股、半导体、盘口情绪、产业趋势",
        "weight": "题材层",
        "usage": "与研报、财报、公告交叉验证后再纳入股票池",
    },
    {
        "name": "滚雪球的猫菲特闲唠嗑",
        "type": "微信公众号",
        "focus": "盘面闲谈、情绪周期、热点拆解",
        "weight": "辅助层",
        "usage": "用于补充交易温度和叙事变化",
    },
]


REPORT_SOURCES: list[dict[str, str]] = [
    {
        "title": "中信证券 2026 下半年A股策略",
        "url": "https://finance.sina.com.cn/stock/quanshang/2026-05-27/doc-inhzhxzw9351188.shtml",
        "tag": "策略",
    },
    {
        "title": "储能行业深度报告",
        "url": "https://pdf.dfcfw.com/pdf/H3_AP202606041823230049_1.pdf",
        "tag": "储能",
    },
    {
        "title": "国信证券人形机器人专题",
        "url": "https://pdf.dfcfw.com/pdf/H3_AP202502211643350790_1.pdf",
        "tag": "机器人",
    },
    {
        "title": "德勤 2026 全球半导体行业趋势",
        "url": "https://www.deloitte.com/cn/zh/Industries/telecom-media-entertainment/perspectives/deloitte-2026-global-semiconductor-industry-outlook.html",
        "tag": "半导体",
    },
    {
        "title": "宁德时代 2026 一季报",
        "url": "https://static.cninfo.com.cn/finalpage/2026-04-16/1225107946.PDF",
        "tag": "财报",
    },
    {
        "title": "中芯国际 2026 一季报",
        "url": "https://static.cninfo.com.cn/finalpage/2026-05-15/1225307101.PDF",
        "tag": "财报",
    },
]


CATALYST_TAXONOMY: dict[str, tuple[str, ...]] = {
    "SUPPLY_DISRUPTION": ("地震", "停产", "停工", "供应中断", "断供", "减产", "事故", "不可抗力"),
    "PRICE_INCREASE": ("涨价", "提价", "上调价格", "价格上调", "报价上调", "价格上涨", "价格调涨"),
    "DOMESTIC_SUBSTITUTION": ("国产替代", "自主可控", "进口替代", "出口管制", "实体清单", "对日替代"),
    "POLICY_SUPPORT": ("政策支持", "支持发展", "专项资金", "试点", "行动方案", "指导意见", "加快建设"),
    "LIQUIDITY": ("降准", "降息", "逆回购", "流动性", "公开市场操作", "两融"),
    "ORDER_CAPEX": ("订单", "中标", "合同", "资本开支", "扩产", "产能建设", "招标", "定点"),
    "DEMAND_BOOM": ("需求旺盛", "供不应求", "需求增长", "景气度", "销量增长", "排产提升"),
    "TECH_PRODUCT": ("首发", "首次", "发布", "量产", "技术突破", "获批", "临床", "上线", "播出"),
    "EARNINGS": ("业绩预告", "业绩快报", "财报", "营收增长", "净利润", "扭亏", "预增"),
    "EVENT_CALENDAR": ("大会", "会议", "展会", "开幕", "峰会", "发布会", "ChinaJoy"),
    "M_AND_A": ("并购", "重组", "收购", "资产注入", "控制权变更"),
    "OVERSEAS_STRESS": ("美股大跌", "纳指大跌", "韩股大跌", "韩国股市", "全球股市下跌", "熔断", "VIX"),
    "NEGATIVE_FUNDAMENTALS": (
        "砍单",
        "下调资本开支",
        "资本开支下调",
        "资本开支不支持",
        "资本开支放缓",
        "资本支出低于预期",
        "资本支出规模低于预期",
        "资本支出放缓",
        "需求不及预期",
        "长协价不涨",
        "无法涨价",
        "无法上涨",
        "盈利下滑",
        "业绩下修",
        "库存高企",
    ),
    "REGULATORY_RISK": ("立案", "处罚", "退市风险", "问询函", "减持", "解禁"),
}

CATALYST_LABELS = {
    "SUPPLY_DISRUPTION": "供给扰动",
    "PRICE_INCREASE": "涨价",
    "DOMESTIC_SUBSTITUTION": "国产替代",
    "POLICY_SUPPORT": "政策",
    "LIQUIDITY": "流动性",
    "ORDER_CAPEX": "订单/资本开支",
    "DEMAND_BOOM": "需求景气",
    "TECH_PRODUCT": "产品/技术",
    "EARNINGS": "业绩",
    "EVENT_CALENDAR": "事件日历",
    "M_AND_A": "并购重组",
    "OVERSEAS_STRESS": "外盘压力",
    "NEGATIVE_FUNDAMENTALS": "逻辑转弱",
    "REGULATORY_RISK": "监管风险",
}

SCAN_DIMENSIONS = [
    "外盘与风险偏好",
    "宏观政策与流动性",
    "供给扰动与涨价",
    "国产替代与贸易摩擦",
    "订单、扩产与资本开支",
    "需求景气与库存周期",
    "新产品与技术突破",
    "财报、公告与并购",
    "会议、展会与事件日历",
    "热门板块、资金与龙头验证",
]

THEMES: list[dict[str, Any]] = [
    {
        "id": "market_risk",
        "name": "外盘风险与A股独立性",
        "category": "市场环境",
        "trend": "每日必扫",
        "evidence_level": "外盘与A股盘面交叉验证",
        "base_score": 55,
        "logic": "先判断美股、韩股、日股及美元美债冲击，再观察A股能否由低开修复、成交额和领涨主线体现独立性。",
        "verification": "看隔夜指数、VIX、人民币、A股开盘缺口、成交额、涨跌家数及午后承接。",
        "risk": "外围下跌向汇率、北向情绪和高估值成长扩散，反弹只有一天惯性。",
        "keywords": ["美股", "纳指", "标普", "韩股", "韩国股市", "日经", "外盘", "VIX", "美债", "风险偏好"],
        "stocks": [],
    },
    {
        "id": "policy_liquidity",
        "name": "政策与流动性",
        "category": "市场环境",
        "trend": "每日必扫",
        "evidence_level": "政策原文与资金价格验证",
        "base_score": 55,
        "logic": "宏观政策、央行流动性和资本市场制度变化决定风险偏好与顺周期、金融权重的承接能力。",
        "verification": "看政策原文、公开市场投放、利率、两融余额、宽基ETF和券商银行量价。",
        "risk": "政策预期先交易、实际力度或落地节奏不及预期。",
        "keywords": ["降准", "降息", "逆回购", "流动性", "两融", "资本市场", "稳增长", "政策支持"],
        "stocks": [
            {"code": "600030", "name": "中信证券", "role": "券商权重", "tier": "市场验证"},
            {"code": "601318", "name": "中国平安", "role": "非银金融", "tier": "市场验证"},
        ],
    },
    {
        "id": "semi_materials_japan",
        "name": "对日半导体材料替代",
        "category": "产业主线",
        "trend": "供给扰动与国产替代",
        "evidence_level": "事件、涨价与盘面三重验证",
        "base_score": 61,
        "supply_shock_beneficiary": True,
        "logic": "中日关系、出口限制或日本地震停产会放大关键材料供应安全需求，六氟化钨、电子特气、锆材料等国产替代环节受益。",
        "verification": "看日本工厂复产时间、国内厂商认证与订单、产品报价，以及龙头在指数反弹中的持续领涨能力。",
        "risk": "海外工厂快速复产、国产产品尚未完成客户验证、涨价只停留在情绪交易。",
        "keywords": [
            "六氟化钨",
            "WF6",
            "电子特气",
            "特种气体",
            "半导体材料",
            "氢氧化锆",
            "氧化锆",
            "锆材料",
            "熊本",
            "日本半导体",
            "对日替代",
        ],
        "stocks": [
            {"code": "688146", "name": "中船特气", "role": "电子特气", "tier": "产业验证"},
            {"code": "300346", "name": "南大光电", "role": "电子材料/特气", "tier": "产业验证"},
            {"code": "688268", "name": "华特气体", "role": "电子特气", "tier": "弹性跟踪"},
            {"code": "688106", "name": "金宏气体", "role": "电子气体", "tier": "弹性跟踪"},
            {"code": "300285", "name": "国瓷材料", "role": "锆材料/电子陶瓷", "tier": "产业验证"},
            {"code": "002167", "name": "东方锆业", "role": "锆制品", "tier": "弹性跟踪"},
            {"code": "603663", "name": "三祥新材", "role": "锆材料", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "mlcc_passive",
        "name": "MLCC与被动元件涨价",
        "category": "产业主线",
        "trend": "涨价与AI需求",
        "evidence_level": "报价、订单和库存验证",
        "base_score": 59,
        "supply_shock_beneficiary": True,
        "logic": "AI服务器和高端电子提升大容量MLCC需求，海外龙头涨价叠加供给偏紧，向国内MLCC、陶瓷粉体和离型膜环节传导。",
        "verification": "看厂商正式调价函、订单锁量、库存天数、稼动率及国内厂商毛利率。",
        "risk": "涨价限于局部规格、下游提前备货后需求回落、国内产品结构偏中低端。",
        "keywords": ["MLCC", "积层陶瓷电容", "片式电容", "被动元件", "三星电机", "村田", "陶瓷粉体", "离型膜"],
        "stocks": [
            {"code": "000636", "name": "风华高科", "role": "MLCC", "tier": "产业验证"},
            {"code": "300408", "name": "三环集团", "role": "电子陶瓷/MLCC", "tier": "产业验证"},
            {"code": "300285", "name": "国瓷材料", "role": "MLCC陶瓷粉体", "tier": "产业验证"},
            {"code": "002859", "name": "洁美科技", "role": "离型膜/纸带", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "ai_apps",
        "name": "AI应用、软件与内容",
        "category": "产业主线",
        "trend": "产品落地与应用扩散",
        "evidence_level": "用户、收入和付费验证",
        "base_score": 58,
        "logic": "AI电视剧、短剧、游戏、办公软件和营销工具的上线，把AI叙事从算力投入推进到内容生产效率、用户增长与商业化。",
        "verification": "看产品上线、播放与活跃用户、付费率、广告收入、推理成本和软件公司订单。",
        "risk": "只有演示没有付费，内容合规与版权风险，海外软件映射到A股后业绩兑现较弱。",
        "keywords": ["AI应用", "AI电视剧", "人工智能电视剧", "AI短剧", "AI影视", "短剧", "软件股", "SaaS", "AI游戏", "AI营销", "AI办公"],
        "stocks": [
            {"code": "300418", "name": "昆仑万维", "role": "大模型/应用", "tier": "产品验证"},
            {"code": "300624", "name": "万兴科技", "role": "创意软件", "tier": "产品验证"},
            {"code": "300364", "name": "中文在线", "role": "数字内容/IP", "tier": "弹性跟踪"},
            {"code": "300133", "name": "华策影视", "role": "影视内容", "tier": "弹性跟踪"},
            {"code": "300058", "name": "蓝色光标", "role": "AI营销", "tier": "弹性跟踪"},
            {"code": "603533", "name": "掌阅科技", "role": "数字阅读/内容", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "semiconductor_equipment",
        "name": "半导体设备、光刻与先进封装",
        "category": "产业主线",
        "trend": "国产替代中线",
        "evidence_level": "招标、验证和收入兑现",
        "base_score": 60,
        "logic": "出口限制、晶圆厂扩产和先进封装需求共同推动设备、光刻材料、测试与封装国产化。",
        "verification": "看晶圆厂招标、客户验证、设备收入、先进封装订单和国产化率。",
        "risk": "验证周期长、扩产节奏下修、估值偏高、订单确认滞后。",
        "keywords": ["半导体设备", "光刻机", "光刻胶", "先进封装", "晶圆厂", "刻蚀", "薄膜沉积", "CMP", "出口管制"],
        "stocks": [
            {"code": "002371", "name": "北方华创", "role": "半导体设备", "tier": "核心验证"},
            {"code": "688012", "name": "中微公司", "role": "刻蚀/MOCVD", "tier": "核心验证"},
            {"code": "688120", "name": "华海清科", "role": "CMP设备", "tier": "核心验证"},
            {"code": "688072", "name": "拓荆科技", "role": "薄膜沉积", "tier": "核心验证"},
            {"code": "688037", "name": "芯源微", "role": "涂胶显影", "tier": "产业验证"},
            {"code": "600584", "name": "长电科技", "role": "先进封装", "tier": "产业验证"},
        ],
    },
    {
        "id": "ai_compute",
        "name": "AI海外链、算力与光通信",
        "category": "产业主线",
        "trend": "高景气但需持续证伪",
        "evidence_level": "资本开支、长协价和订单验证",
        "base_score": 62,
        "logic": "海外云厂商资本开支决定AI服务器、光模块、PCB和液冷的增长斜率；长协价格、订单和盈利若下修，主线会从成长交易转为估值消化。",
        "verification": "看云厂商资本开支指引、800G/1.6T出货、长协价格、订单能见度、毛利率与库存。",
        "risk": "资本开支不支持高增长、长协无法涨价、砍单、出口限制和高估值回撤。",
        "keywords": ["算力", "光模块", "CPO", "AI服务器", "PCB", "液冷", "数据中心", "云厂商", "资本开支", "资本支出", "长协"],
        "stocks": [
            {"code": "300308", "name": "中际旭创", "role": "光模块", "tier": "核心验证"},
            {"code": "300502", "name": "新易盛", "role": "光模块", "tier": "核心验证"},
            {"code": "300394", "name": "天孚通信", "role": "光器件", "tier": "核心验证"},
            {"code": "601138", "name": "工业富联", "role": "AI服务器", "tier": "核心验证"},
            {"code": "002463", "name": "沪电股份", "role": "AI PCB", "tier": "核心验证"},
            {"code": "002837", "name": "英维克", "role": "数据中心液冷", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "consumer",
        "name": "消费、文旅与服务业",
        "category": "轮动方向",
        "trend": "政策与业绩双驱动",
        "evidence_level": "客流、同店和价格验证",
        "base_score": 54,
        "logic": "促消费政策、暑期旺季、文旅客流和服务消费改善可驱动商贸零售、酒店旅游、食品饮料轮动。",
        "verification": "看客流、同店销售、入住率、免税销售、价格和中报业绩。",
        "risk": "一次性节假日脉冲、居民消费意愿偏弱、行业价格战。",
        "keywords": ["消费", "文旅", "旅游", "酒店", "零售", "免税", "餐饮", "暑期", "以旧换新", "服务消费"],
        "stocks": [
            {"code": "601888", "name": "中国中免", "role": "免税", "tier": "行业验证"},
            {"code": "600754", "name": "锦江酒店", "role": "酒店", "tier": "行业验证"},
            {"code": "600859", "name": "王府井", "role": "商贸零售", "tier": "弹性跟踪"},
            {"code": "600258", "name": "首旅酒店", "role": "酒店旅游", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "brain_computer",
        "name": "脑机接口与医疗科技",
        "category": "事件主题",
        "trend": "政策与临床里程碑",
        "evidence_level": "临床、注册和订单验证",
        "base_score": 52,
        "logic": "政策、临床试验和产品注册推动脑机接口从概念向医疗康复、信号采集和人机交互落地。",
        "verification": "看临床入组、注册证、医院订单、核心器件收入和伦理合规。",
        "risk": "产业化周期长、概念收入占比低、临床结果不确定。",
        "keywords": ["脑机接口", "脑科学", "神经调控", "植入式", "脑电", "神经康复"],
        "stocks": [
            {"code": "002173", "name": "创新医疗", "role": "脑机接口合作", "tier": "事件跟踪"},
            {"code": "300003", "name": "乐普医疗", "role": "医疗器械", "tier": "产业观察"},
            {"code": "600775", "name": "南京熊猫", "role": "脑机交互概念", "tier": "题材验证"},
        ],
    },
    {
        "id": "humanoid_robot",
        "name": "人形机器人核心零部件",
        "category": "产业主线",
        "trend": "量产预期高弹性",
        "evidence_level": "定点、量产和收入验证",
        "base_score": 58,
        "logic": "整机量产预期向执行器、丝杠、减速器、传感器、灵巧手和结构件扩散。",
        "verification": "看定点订单、客户认证、量产时间表、机器人收入占比和毛利率。",
        "risk": "量产节奏低于预期、概念收入占比低、交易拥挤。",
        "keywords": ["人形机器人", "机器人", "灵巧手", "丝杠", "减速器", "力传感器", "执行器"],
        "stocks": [
            {"code": "002050", "name": "三花智控", "role": "执行器", "tier": "核心验证"},
            {"code": "601689", "name": "拓普集团", "role": "执行器/零部件", "tier": "核心验证"},
            {"code": "688017", "name": "绿的谐波", "role": "减速器", "tier": "弹性跟踪"},
            {"code": "603667", "name": "五洲新春", "role": "轴承/丝杠", "tier": "弹性跟踪"},
            {"code": "603662", "name": "柯力传感", "role": "力传感器", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "commercial_space",
        "name": "商业航天与卫星互联网",
        "category": "事件主题",
        "trend": "发射与订单驱动",
        "evidence_level": "发射计划、招标和交付验证",
        "base_score": 55,
        "logic": "火箭发射、卫星组网和地面终端建设带动制造、测控、通信载荷及材料环节。",
        "verification": "看发射频次、星座招标、卫星交付、在手订单与商业化收入。",
        "risk": "发射延期、概念业务占比低、订单确认周期长。",
        "keywords": ["商业航天", "卫星互联网", "火箭", "低轨卫星", "星座", "卫星发射", "航天"],
        "stocks": [
            {"code": "600118", "name": "中国卫星", "role": "卫星制造", "tier": "产业验证"},
            {"code": "601698", "name": "中国卫通", "role": "卫星运营", "tier": "产业验证"},
            {"code": "300053", "name": "航宇微", "role": "卫星数据/芯片", "tier": "弹性跟踪"},
            {"code": "688066", "name": "航天宏图", "role": "卫星应用", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "energy_power",
        "name": "储能、电网与数据中心电力",
        "category": "产业主线",
        "trend": "需求与订单驱动",
        "evidence_level": "订单、毛利和装机验证",
        "base_score": 57,
        "logic": "新型电力系统、海外大储与AI数据中心电力需求带动储能、电网设备、UPS和液冷。",
        "verification": "看海外大储订单、PCS毛利率、电网投资、数据中心配电与液冷订单。",
        "risk": "价格竞争、海外政策波动、库存周期、盈利下行。",
        "keywords": ["储能", "电网", "特高压", "PCS", "逆变器", "数据中心电力", "UPS", "液冷", "电力设备"],
        "stocks": [
            {"code": "300750", "name": "宁德时代", "role": "储能电池", "tier": "核心验证"},
            {"code": "300274", "name": "阳光电源", "role": "逆变器/储能系统", "tier": "核心验证"},
            {"code": "300693", "name": "盛弘股份", "role": "PCS/电力电子", "tier": "弹性跟踪"},
            {"code": "002335", "name": "科华数据", "role": "数据中心电力", "tier": "弹性跟踪"},
            {"code": "002837", "name": "英维克", "role": "液冷", "tier": "弹性跟踪"},
        ],
    },
    {
        "id": "innovative_drug",
        "name": "创新药、BD与AI医疗",
        "category": "产业主线",
        "trend": "临床与出海兑现",
        "evidence_level": "临床、BD和收入验证",
        "base_score": 56,
        "logic": "临床数据、海外授权BD、创新药政策和AI医疗产品推动医药成长资产重估。",
        "verification": "看临床终点、授权首付款、销售放量、医保谈判和研发费用效率。",
        "risk": "临床失败、授权不确定、集采与医保控费、估值波动。",
        "keywords": ["创新药", "BD", "授权出海", "临床试验", "新药获批", "AI医疗", "医保"],
        "stocks": [
            {"code": "600276", "name": "恒瑞医药", "role": "创新药", "tier": "核心验证"},
            {"code": "688506", "name": "百利天恒", "role": "创新药/BD", "tier": "产业验证"},
            {"code": "300347", "name": "泰格医药", "role": "CRO", "tier": "景气验证"},
            {"code": "300760", "name": "迈瑞医疗", "role": "医疗器械", "tier": "行业验证"},
        ],
    },
    {
        "id": "solid_state_battery",
        "name": "固态电池与新材料",
        "category": "事件主题",
        "trend": "技术和量产节点驱动",
        "evidence_level": "样品、验证和产线验证",
        "base_score": 53,
        "logic": "固态电池样车、样品验证和中试线进度推动固态电解质、硅碳负极及设备材料交易。",
        "verification": "看样品参数、车企验证、中试线、良率、成本和真实收入。",
        "risk": "量产时间推迟、技术路线变化、概念收入极低。",
        "keywords": ["固态电池", "半固态", "固态电解质", "硫化物电解质", "硅碳负极", "锂金属"],
        "stocks": [
            {"code": "300750", "name": "宁德时代", "role": "电池技术", "tier": "产业验证"},
            {"code": "300014", "name": "亿纬锂能", "role": "电池技术", "tier": "产业验证"},
            {"code": "688567", "name": "孚能科技", "role": "半固态电池", "tier": "弹性跟踪"},
            {"code": "603663", "name": "三祥新材", "role": "固态电解质材料", "tier": "题材验证"},
        ],
    },
    {
        "id": "intelligent_mobility",
        "name": "智能驾驶、低空经济与交通科技",
        "category": "事件主题",
        "trend": "政策试点与产品落地",
        "evidence_level": "牌照、订单和运营验证",
        "base_score": 53,
        "logic": "自动驾驶试点、Robotaxi运营、低空政策和eVTOL适航节点带动感知、域控、运营和空管产业链。",
        "verification": "看试点牌照、运营里程、量产定点、适航证、订单和商业化收入。",
        "risk": "政策与适航延期、安全事故、商业模式尚未闭环。",
        "keywords": ["自动驾驶", "Robotaxi", "无人驾驶", "智能驾驶", "低空经济", "eVTOL", "飞行汽车", "适航"],
        "stocks": [
            {"code": "002920", "name": "德赛西威", "role": "智能座舱/域控", "tier": "产业验证"},
            {"code": "300496", "name": "中科创达", "role": "智能驾驶软件", "tier": "产业验证"},
            {"code": "002405", "name": "四维图新", "role": "高精地图/智驾", "tier": "弹性跟踪"},
            {"code": "600038", "name": "中直股份", "role": "航空制造", "tier": "产业观察"},
        ],
    },
    {
        "id": "resource_price",
        "name": "资源品与化工涨价",
        "category": "周期方向",
        "trend": "供需与价格驱动",
        "evidence_level": "现货、库存和价差验证",
        "base_score": 54,
        "supply_shock_beneficiary": True,
        "logic": "供给收缩、事故检修、地缘扰动或需求改善推动金属、化工和细分材料价格上行。",
        "verification": "看现货期货价格、库存、开工率、价差、企业调价函和业绩弹性。",
        "risk": "涨价持续性不足、下游抵触、复产后供给恢复、期货价格反转。",
        "keywords": ["黄金", "白银", "铜", "稀土", "有色", "化工", "氟化工", "锂盐", "纯碱", "涨价"],
        "stocks": [
            {"code": "601899", "name": "紫金矿业", "role": "铜金", "tier": "价格验证"},
            {"code": "600547", "name": "山东黄金", "role": "黄金", "tier": "价格验证"},
            {"code": "600111", "name": "北方稀土", "role": "稀土", "tier": "价格验证"},
            {"code": "600160", "name": "巨化股份", "role": "氟化工", "tier": "景气验证"},
        ],
    },
    {
        "id": "dividend_defense",
        "name": "高股息与防御资产",
        "category": "风格方向",
        "trend": "风险偏好对冲",
        "evidence_level": "股息、现金流和相对强度验证",
        "base_score": 50,
        "logic": "外盘压力、市场缩量或成长主线退潮时，银行、公用事业、运营商等高股息资产可能承接防御资金。",
        "verification": "看长端利率、股息率、现金流、宽基与红利ETF流量及相对强度。",
        "risk": "风险偏好快速修复、利率上行、分红不及预期。",
        "keywords": ["高股息", "红利", "银行", "运营商", "公用事业", "煤炭", "防御"],
        "stocks": [
            {"code": "601398", "name": "工商银行", "role": "银行红利", "tier": "风格验证"},
            {"code": "600941", "name": "中国移动", "role": "运营商红利", "tier": "风格验证"},
            {"code": "600900", "name": "长江电力", "role": "公用事业", "tier": "风格验证"},
        ],
    },
]


def _query(engine: Any, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        query_engine = get_kline_engine() if should_use_kline_engine(sql) else engine
        with query_engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row) for row in result.mappings().all()]
    except Exception:
        return []


def _latest_trade_date(engine: Any, fallback: str | None = None) -> str:
    rows = _query(
        engine,
        """
        SELECT MAX(trade_date) AS d
        FROM sm_stock_kline
        WHERE k_type = 1
        """,
    )
    if rows and rows[0].get("d"):
        return str(rows[0]["d"])[:10]
    return fallback or date.today().isoformat()


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:19])
    except ValueError:
        return None


def _radar_cutoff(trade_date: str) -> datetime:
    try:
        target_date = date.fromisoformat(str(trade_date)[:10])
    except ValueError:
        target_date = date.today()
    now = datetime.now()
    if target_date >= now.date():
        return now
    return datetime.combine(target_date, time.max)


def _trigger_types(content: str) -> list[str]:
    normalized = _normalize(content)
    return [
        trigger_type
        for trigger_type, words in CATALYST_TAXONOMY.items()
        if any(_normalize(word) in normalized for word in words)
    ]


def _matched_theme_ids(content: str) -> list[str]:
    normalized = _normalize(content)
    matched = []
    for theme in THEMES:
        if not any(_normalize(keyword) in normalized for keyword in theme.get("keywords", [])):
            continue
        if theme["id"] == "consumer":
            energy_context = any(
                word in normalized
                for word in ("能源消费", "电力消费", "绿电消费", "可再生能源消费", "原油消费")
            )
            consumer_context = any(
                word in normalized
                for word in ("零售", "旅游", "酒店", "免税", "餐饮", "文旅", "服务消费", "促消费", "消费品")
            )
            if energy_context and not consumer_context:
                continue
        matched.append(theme["id"])
    return matched


def classify_news_catalysts(title: str, content: str = "") -> dict[str, Any]:
    """Classify a headline into every matched theme and catalyst type.

    This pure helper is deliberately public so ingestion and tests can use the
    same coverage rules as the recommendation page.
    """
    raw_text = f"{title or ''} {content or ''}".strip()
    title_theme_ids = _matched_theme_ids(title)
    theme_ids = _matched_theme_ids(raw_text)
    trigger_types = _trigger_types(raw_text)
    return {
        "theme_ids": theme_ids,
        "title_theme_ids": title_theme_ids,
        "trigger_types": trigger_types,
        "trigger_labels": [CATALYST_LABELS.get(item, item) for item in trigger_types],
        "negative": bool({"NEGATIVE_FUNDAMENTALS", "REGULATORY_RISK"} & set(trigger_types)),
    }


def _source_reliability(source: Any, content: str) -> float:
    value = f"{source or ''} {content}"
    if any(
        name in value
        for name in (
            "国务院",
            "证监会",
            "财政部",
            "人民银行",
            "国家发改委",
            "上交所",
            "深交所",
            "新华社",
            "公司公告",
        )
    ):
        return 1.0
    if any(name in value for name in ("财联社", "证券时报", "中国证券报", "上海证券报", "央视")):
        return 0.88
    return 0.68


def _freshness_weight(publish_time: Any, cutoff: datetime) -> float:
    published = _parse_datetime(publish_time)
    if published is None:
        return 0.45
    age_hours = max(0.0, (cutoff - published).total_seconds() / 3600.0)
    return max(0.16, math.pow(0.5, age_hours / 30.0))


def _importance_weight(row: dict[str, Any]) -> float:
    value = 1.0
    if int(row.get("is_top") or 0) == 1:
        value += 0.28
    if int(row.get("jpush") or 0) == 1:
        value += 0.18
    if str(row.get("level") or "").upper() in {"A", "1", "HIGH", "IMPORTANT"}:
        value += 0.22
    return value


def _news_direction(theme: dict[str, Any], content: str, trigger_types: list[str]) -> float:
    trigger_set = set(trigger_types)
    normalized = _normalize(content)
    if trigger_set & {"NEGATIVE_FUNDAMENTALS", "REGULATORY_RISK"}:
        return -1.0
    if "OVERSEAS_STRESS" in trigger_set:
        if theme["id"] == "dividend_defense":
            return 0.35
        return -1.0 if theme["id"] in {"market_risk", "ai_compute"} else -0.35
    if "SUPPLY_DISRUPTION" in trigger_set:
        foreign_shock = any(word in normalized for word in ("日本", "韩国", "海外", "进口", "熊本"))
        if theme.get("supply_shock_beneficiary") and foreign_shock:
            return 1.0
        return -0.45
    if trigger_set & {
        "PRICE_INCREASE",
        "DOMESTIC_SUBSTITUTION",
        "POLICY_SUPPORT",
        "LIQUIDITY",
        "ORDER_CAPEX",
        "DEMAND_BOOM",
        "TECH_PRODUCT",
        "EARNINGS",
        "EVENT_CALENDAR",
        "M_AND_A",
    }:
        if any(word in normalized for word in ("下滑", "下降", "亏损", "不及预期", "终止", "失败")):
            return -0.7
        return 1.0
    return 0.25


def _load_market_rows(engine: Any, trade_date: str) -> list[dict[str, Any]]:
    return _query(
        engine,
        """
        SELECT concept_name, change_pct, hot_value, plate_type, `rank`, snapshot_date
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM st_hot_concept_ths_daily
            WHERE snapshot_date <= :trade_date
        )
        ORDER BY plate_type, `rank`
        LIMIT 240
        """,
        {"trade_date": trade_date},
    )


def _load_hot_stock_rows(engine: Any, trade_date: str) -> list[dict[str, Any]]:
    return _query(
        engine,
        """
        SELECT f.stock_code, f.short_name, f.change_pct, f.fused_rank, f.snapshot_date
        FROM st_hot_rank_fused f
        WHERE f.snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM st_hot_rank_fused
            WHERE snapshot_date <= :trade_date
        )
        ORDER BY f.fused_rank
        LIMIT 500
        """,
        {"trade_date": trade_date},
    )


def _load_news_rows(engine: Any, cutoff: datetime, lookback_hours: int = 72) -> list[dict[str, Any]]:
    return _query(
        engine,
        """
        SELECT source, title, content, publish_time, level, is_top, jpush
        FROM st_news_flash
        WHERE publish_time >= :start_time
          AND publish_time <= :cutoff_time
        ORDER BY publish_time DESC
        LIMIT 20000
        """,
        {
            "start_time": cutoff - timedelta(hours=lookback_hours),
            "cutoff_time": cutoff,
        },
    )


def _theme_market_hits(theme: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keywords = [_normalize(keyword) for keyword in theme.get("keywords", [])]
    hits = []
    for row in rows:
        name = str(row.get("concept_name") or "")
        normalized = _normalize(name)
        if not any(keyword and keyword in normalized for keyword in keywords):
            continue
        hits.append(
            {
                "name": name,
                "change_pct": _as_float(row.get("change_pct")),
                "hot_value": _as_float(row.get("hot_value")),
                "rank": row.get("rank"),
                "snapshot_date": str(row.get("snapshot_date") or "")[:10],
            }
        )
    return hits[:8]


def _theme_stock_hits(theme: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    code_set = {str(stock.get("code") or "").zfill(6) for stock in theme.get("stocks", [])}
    hits = []
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        if code not in code_set:
            continue
        hits.append(
            {
                "code": code,
                "name": row.get("short_name") or "",
                "change_pct": _as_float(row.get("change_pct")),
                "rank": row.get("fused_rank"),
                "snapshot_date": str(row.get("snapshot_date") or "")[:10],
            }
        )
    return hits[:10]


def _theme_relevant_excerpt(theme: dict[str, Any], title: str, content: str) -> tuple[str, str, bool]:
    title_normalized = _normalize(title)
    if any(_normalize(keyword) in title_normalized for keyword in theme.get("keywords", [])):
        return title or content[:160], f"{title} {content[:500]}", True

    lowered_content = str(content or "").lower()
    positions = []
    for keyword in theme.get("keywords", []):
        position = lowered_content.find(str(keyword).lower())
        if position >= 0:
            positions.append(position)
    if not positions:
        return title or content[:160], f"{title} {content[:500]}", False
    position = min(positions)
    start = max(0, position - 100)
    end = min(len(content), position + 320)
    excerpt = str(content[start:end]).strip(" \n\r\t；;。")
    display = excerpt[:160] if excerpt else (title or content[:160])
    return display, excerpt, False


def _classify_news_rows(
    rows: list[dict[str, Any]],
    cutoff: datetime,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unclassified = []
    trigger_types_seen: set[str] = set()
    seen_titles: set[str] = set()
    theme_map = {theme["id"]: theme for theme in THEMES}

    for row in rows:
        title = str(row.get("title") or "").strip()
        content = str(row.get("content") or "").strip()
        display_title = title or content[:120]
        dedupe_key = _normalize(display_title)
        if not dedupe_key or dedupe_key in seen_titles:
            continue
        seen_titles.add(dedupe_key)
        classification = classify_news_catalysts(title, content[:1000])
        global_trigger_types = classification["trigger_types"]
        trigger_types_seen.update(global_trigger_types)
        strength = (
            _source_reliability(row.get("source"), f"{title} {content[:300]}")
            * _freshness_weight(row.get("publish_time"), cutoff)
            * _importance_weight(row)
        )
        base_item = {
            "title": display_title[:160],
            "source": str(row.get("source") or ""),
            "publish_time": str(row.get("publish_time") or "")[:19],
            "trigger_types": global_trigger_types,
            "trigger_labels": [CATALYST_LABELS.get(item, item) for item in global_trigger_types],
            "strength": round(strength, 4),
        }
        if not classification["theme_ids"]:
            if global_trigger_types and strength >= 0.48:
                unclassified.append(base_item)
            continue
        for theme_id in classification["theme_ids"]:
            theme = theme_map[theme_id]
            relevant_title, relevant_text, title_theme_match = _theme_relevant_excerpt(
                theme,
                title,
                content,
            )
            trigger_types = _trigger_types(relevant_text)
            direction = _news_direction(theme, relevant_text, trigger_types)
            theme_strength = strength if title_theme_match else strength * 0.84
            by_theme[theme_id].append(
                {
                    **base_item,
                    "title": relevant_title[:160],
                    "trigger_types": trigger_types,
                    "trigger_labels": [CATALYST_LABELS.get(item, item) for item in trigger_types],
                    "title_theme_match": title_theme_match,
                    "strength": round(theme_strength, 4),
                    "direction": direction,
                    "impact_score": round(theme_strength * direction, 4),
                }
            )
    return by_theme, unclassified[:15], sorted(trigger_types_seen)


def _theme_score(
    theme: dict[str, Any],
    market_hits: list[dict[str, Any]],
    stock_hits: list[dict[str, Any]],
    news_hits: list[dict[str, Any]],
) -> tuple[int, float]:
    base = _as_float(theme.get("base_score"), 50.0)
    raw_news = sum(_as_float(item.get("impact_score")) for item in news_hits)
    news_signal = math.tanh(raw_news / 1.8) if news_hits else 0.0
    market_avg = (
        sum(_as_float(item.get("change_pct")) for item in market_hits) / len(market_hits)
        if market_hits
        else 0.0
    )
    stock_avg = (
        sum(_as_float(item.get("change_pct")) for item in stock_hits) / len(stock_hits)
        if stock_hits
        else 0.0
    )
    market_bonus = min(11.0, len(market_hits) * 1.8 + max(-4.0, min(5.0, market_avg)) * 0.9)
    stock_bonus = min(9.0, len(stock_hits) * 1.2 + max(-4.0, min(5.0, stock_avg)) * 0.55)
    evidence_bonus = min(4.0, len(news_hits) * 0.7)
    score = round(base + news_signal * 18.0 + market_bonus + stock_bonus + evidence_bonus)
    return max(30, min(96, score)), round(news_signal, 4)


def _theme_rank_tier(score: int, status: str) -> str:
    if status == "逻辑转弱":
        return "风险观察"
    if score >= 88:
        return "S"
    if score >= 78:
        return "A"
    if score >= 68:
        return "B"
    return "观察"


def _theme_status(
    score: int,
    news_signal: float,
    market_hits: list[dict[str, Any]],
    stock_hits: list[dict[str, Any]],
    news_hits: list[dict[str, Any]],
) -> str:
    decisive_negative = any(
        _as_float(item.get("impact_score")) <= -0.65
        and bool(item.get("title_theme_match"))
        and bool(
            {"NEGATIVE_FUNDAMENTALS", "REGULATORY_RISK"}
            & set(item.get("trigger_types") or [])
        )
        for item in news_hits
    )
    if decisive_negative or news_signal <= -0.28:
        return "逻辑转弱"
    has_evidence = bool(market_hits or stock_hits or news_hits)
    if score >= 78 and has_evidence:
        return "主线候选"
    if score >= 68 and has_evidence:
        return "轮动候选"
    if has_evidence:
        return "事件观察"
    return "常规观察"


def build_research_radar(engine: Any = None, trade_date: str | None = None) -> dict[str, Any]:
    """Build a full-market catalyst pool and ranked research radar."""
    active_trade_date = str(trade_date or _latest_trade_date(engine))[:10]
    cutoff = _radar_cutoff(active_trade_date)
    market_rows = _load_market_rows(engine, active_trade_date)
    hot_stock_rows = _load_hot_stock_rows(engine, active_trade_date)
    news_rows = _load_news_rows(engine, cutoff)
    news_by_theme, unclassified, trigger_types_seen = _classify_news_rows(news_rows, cutoff)

    themes = []
    for theme in THEMES:
        market_hits = _theme_market_hits(theme, market_rows)
        stock_hits = _theme_stock_hits(theme, hot_stock_rows)
        news_hits = sorted(
            news_by_theme.get(theme["id"], []),
            key=lambda item: abs(_as_float(item.get("impact_score"))),
            reverse=True,
        )[:8]
        score, news_signal = _theme_score(theme, market_hits, stock_hits, news_hits)
        status = _theme_status(score, news_signal, market_hits, stock_hits, news_hits)
        trigger_types = sorted(
            {
                trigger_type
                for item in news_hits
                for trigger_type in item.get("trigger_types", [])
            }
        )
        themes.append(
            {
                **theme,
                "score": score,
                "news_signal": news_signal,
                "status": status,
                "rank_tier": _theme_rank_tier(score, status),
                "active": status != "常规观察",
                "trigger_types": trigger_types,
                "trigger_labels": [CATALYST_LABELS.get(item, item) for item in trigger_types],
                "news_hits": news_hits,
                "market_hits": market_hits,
                "stock_hits": stock_hits,
            }
        )

    status_order = {"主线候选": 0, "轮动候选": 1, "事件观察": 2, "逻辑转弱": 3, "常规观察": 4}
    themes.sort(key=lambda item: (status_order.get(item["status"], 9), -item["score"], item["name"]))
    for index, theme in enumerate(themes, 1):
        theme["rank"] = index

    stock_pool = []
    for theme in themes:
        for stock in theme.get("stocks", []):
            stock_pool.append(
                {
                    **stock,
                    "theme": theme["name"],
                    "theme_id": theme["id"],
                    "theme_score": theme["score"],
                    "theme_status": theme["status"],
                }
            )

    active_themes = [theme for theme in themes if theme["active"]]
    return {
        "trade_date": active_trade_date,
        "cutoff_at": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "research-radar-full-market-v2.0",
        "source_signals": SOURCE_SIGNALS,
        "report_sources": REPORT_SOURCES,
        "scan_dimensions": SCAN_DIMENSIONS,
        "themes": themes,
        "stock_pool": stock_pool,
        "unclassified_catalysts": unclassified,
        "coverage_summary": {
            "scanned_dimension_count": len(SCAN_DIMENSIONS),
            "scanned_theme_count": len(themes),
            "active_theme_count": len(active_themes),
            "weakening_theme_count": sum(theme["status"] == "逻辑转弱" for theme in themes),
            "news_count": len(news_rows),
            "matched_trigger_count": len(trigger_types_seen),
            "unclassified_catalyst_count": len(unclassified),
        },
        "method": (
            "先全市场扫描外盘、政策、供给、涨价、国产替代、订单、技术、业绩和事件日历，"
            "再用热门板块、资金与龙头表现验证；所有主题保留展示，最终买入榜只做二次筛选。"
        ),
        "disclaimer": "仅用于信息整理和研究跟踪，不构成任何买卖建议。",
    }


def _limited_themes(radar: dict[str, Any], max_themes: int | None) -> list[dict[str, Any]]:
    themes = list(radar.get("themes", []))
    if max_themes is None or max_themes <= 0:
        return themes
    return themes[:max_themes]


def format_radar_prompt_block(radar: dict[str, Any], max_themes: int | None = None) -> str:
    rows = []
    for theme in _limited_themes(radar, max_themes):
        stocks = "、".join(stock["name"] for stock in theme.get("stocks", [])[:6]) or "暂无固定映射"
        catalysts = "、".join(theme.get("trigger_labels", [])) or "等待新催化"
        rows.append(
            {
                "排名": theme.get("rank"),
                "层级": theme.get("rank_tier"),
                "状态": theme.get("status"),
                "主线": theme["name"],
                "强度": theme["score"],
                "催化": catalysts,
                "逻辑": theme["logic"],
                "验证": theme["verification"],
                "股票池": stocks,
                "风险": theme["risk"],
            }
        )
    return "\n".join(json_like(row) for row in rows)


def json_like(row: dict[str, Any]) -> str:
    parts = [f"{key}: {value}" for key, value in row.items()]
    return "- " + "；".join(parts)


def format_radar_markdown(
    radar: dict[str, Any],
    title: str = "全市场催化与研报趋势雷达",
    max_themes: int | None = None,
) -> str:
    """Format every scanned theme for WeCom morning/evening reports."""
    summary = radar.get("coverage_summary") or {}
    lines = [f"**{title}**"]
    lines.append(f"> {radar.get('method', '')}")
    lines.append(
        "> 覆盖："
        f"{summary.get('scanned_dimension_count', 0)} 类扫描维度，"
        f"{summary.get('scanned_theme_count', 0)} 个主题，"
        f"{summary.get('active_theme_count', 0)} 个当前有证据，"
        f"{summary.get('weakening_theme_count', 0)} 个逻辑转弱。"
    )
    for theme in _limited_themes(radar, max_themes):
        stocks = "、".join(f"{stock['name']}({stock['code']})" for stock in theme.get("stocks", [])[:4])
        market_hits = theme.get("market_hits") or []
        hit_text = "、".join(
            f"{hit['name']}({_pct(hit.get('change_pct'))})"
            for hit in market_hits[:3]
        ) or "等待盘面验证"
        catalysts = "、".join(theme.get("trigger_labels", [])) or "无新增催化"
        lines.append(
            f"> #{theme.get('rank', '-')} [{theme.get('rank_tier', '观察')}/{theme.get('status', '观察')}] "
            f"{theme['name']} {theme['score']}/100；催化 {catalysts}；"
            f"映射 {stocks or '暂无固定股票池'}；盘面 {hit_text}"
        )
    unclassified = radar.get("unclassified_catalysts") or []
    if unclassified:
        lines.append(
            "> 未归类高优先级催化："
            + "；".join(str(item.get("title") or "") for item in unclassified[:6])
        )
    lines.append("> 风险：热度只是入口，最终以公告、订单、收入、利润率、现金流与盘面持续性为准。")
    return "\n".join(lines)


def _pct(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "-"
    return f"{'+' if number >= 0 else ''}{number:.2f}%"
