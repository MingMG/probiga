# -*- coding: utf-8 -*-
"""
评分计算工具

提供各维度评分的通用计算函数。
所有评分范围为 0-100，越高越好。
"""


def score_roe(roe: float) -> float:
    """ROE评分"""
    if roe > 25: return 95
    if roe > 20: return 85
    if roe > 15: return 75
    if roe > 10: return 60
    if roe > 5: return 40
    if roe > 0: return 20
    return 10


def score_gross_margin(gm: float) -> float:
    """毛利率评分"""
    if gm > 60: return 90
    if gm > 40: return 75
    if gm > 30: return 60
    if gm > 20: return 45
    if gm > 10: return 30
    return 15


def score_asset_liab_ratio(ratio: float) -> float:
    """资产负债率评分（越低越好）"""
    if ratio < 30: return 95
    if ratio < 40: return 80
    if ratio < 50: return 65
    if ratio < 60: return 50
    if ratio < 70: return 35
    return 15


def score_growth_rate(rate: float) -> float:
    """增长率评分"""
    if rate > 50: return 95
    if rate > 30: return 85
    if rate > 20: return 75
    if rate > 10: return 60
    if rate > 0: return 45
    if rate > -10: return 30
    if rate > -30: return 15
    return 5


def score_percentile(percentile: float, reverse: bool = False) -> float:
    """
    分位数评分

    Args:
        percentile: 分位数 (0-100)
        reverse: 是否反转（用于估值，分位越低越便宜，评分越高）
    """
    if percentile is None:
        return 50
    if reverse:
        return 100 - percentile
    return percentile


def score_capital_flow(flow: float, thresholds: dict = None) -> float:
    """
    资金流向评分

    Args:
        flow: 净流入金额（万元）
        thresholds: 自定义阈值
    """
    if thresholds is None:
        thresholds = {
            "strong_buy": 5000,    # >5000万 = 强力买入
            "buy": 1000,           # >1000万 = 买入
            "neutral_high": 0,     # >0 = 中性偏多
            "neutral_low": -1000,  # >-1000万 = 中性偏空
            "sell": -5000,         # >-5000万 = 卖出
        }

    if flow > thresholds["strong_buy"]: return 95
    if flow > thresholds["buy"]: return 80
    if flow > thresholds["neutral_high"]: return 60
    if flow > thresholds["neutral_low"]: return 40
    if flow > thresholds["sell"]: return 20
    return 10


def score_hot_rank(rank: int) -> float:
    """热门排名评分"""
    if rank is None or rank > 100:
        return 30
    if rank <= 10: return 95
    if rank <= 20: return 85
    if rank <= 30: return 75
    if rank <= 50: return 60
    if rank <= 100: return 45
    return 30


def score_rsi(rsi: float) -> float:
    """
    RSI评分

    RSI > 80: 超买，风险高，评分低
    RSI 60-80: 偏强，评分高
    RSI 40-60: 中性
    RSI 20-40: 偏弱
    RSI < 20: 超卖，可能反弹，评分中等
    """
    if rsi is None:
        return 50
    if rsi > 80: return 25
    if rsi > 70: return 45
    if rsi > 60: return 70
    if rsi > 50: return 60
    if rsi > 40: return 50
    if rsi > 30: return 55
    if rsi > 20: return 65
    return 60  # 超卖可能反弹


def score_ma_trend(ma5: float, ma10: float, ma20: float, ma60: float) -> float:
    """均线趋势评分"""
    scores = []

    if ma5 and ma10:
        scores.append(70 if ma5 > ma10 else 30)
    if ma10 and ma20:
        scores.append(70 if ma10 > ma20 else 30)
    if ma20 and ma60:
        scores.append(70 if ma20 > ma60 else 30)

    return sum(scores) / len(scores) if scores else 50


def score_macd(dif: float, dea: float, golden_cross: bool) -> float:
    """MACD评分"""
    if golden_cross:
        return 90
    if dif > 0 and dea > 0:
        return 75
    if dif > 0:
        return 60
    if dif > dea:
        return 45
    return 25


def weighted_average(scores: dict) -> float:
    """
    加权平均

    Args:
        scores: {score: float, weight: float} 的列表
    """
    total_weight = sum(s["weight"] for s in scores.values())
    if total_weight == 0:
        return 50

    weighted_sum = sum(s["score"] * s["weight"] for s in scores.values())
    return round(weighted_sum / total_weight, 1)
