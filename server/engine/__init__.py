# -*- coding: utf-8 -*-
"""
统一股票分析引擎

四层引擎架构：
- LongTermEngine: 长线投资引擎（6个月~3年）
- ShortTermEngine: 短线交易引擎（3~20个交易日）
- EventRiskEngine: 事件风险引擎（实时监控）
- RecommendationGate: 推荐资格引擎（最终裁判）
"""

from .stock_analysis_engine import StockAnalysisEngine
from .schemas import StockAnalysisResult, ScoreDetail, EventRisk, RecommendResult

__all__ = [
    'StockAnalysisEngine',
    'StockAnalysisResult',
    'ScoreDetail',
    'EventRisk',
    'RecommendResult',
]
