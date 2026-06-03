# -*- coding: utf-8 -*-
"""
统一分析引擎入口

所有模块（AI推荐、自选股、全市场、个股详情）共用此引擎，保证：
同一只股票在任何页面看到的分析结果一致。

四层引擎架构：
- LongTermEngine: 长线投资引擎（6个月~3年）
- ShortTermEngine: 短线交易引擎（3~20个交易日）
- EventRiskEngine: 事件风险引擎（实时监控）
- RecommendationGate: 推荐资格引擎（最终裁判）
"""

import logging
from typing import List, Optional

from .schemas import StockAnalysisResult, ScoreDetail, EventRisk, RecommendResult
from .data_loader import StockDataLoader
from .long_term_engine import LongTermEngine
from .short_term_engine import ShortTermEngine
from .event_risk_engine import EventRiskEngine
from .recommendation_gate import RecommendationGate

logger = logging.getLogger(__name__)


class StockAnalysisEngine:
    """
    统一分析引擎

    所有模块共用的分析入口，保证分析结果一致性。
    """

    def __init__(self):
        self.data_loader = StockDataLoader()
        self.long_term = LongTermEngine()
        self.short_term = ShortTermEngine()
        self.event_risk = EventRiskEngine()
        self.gate = RecommendationGate()

    def analyze(self, stock_code: str, full_data: bool = True) -> StockAnalysisResult:
        """
        统一分析入口

        Args:
            stock_code: 股票代码
            full_data: 是否加载完整数据（False时使用精简数据）

        Returns:
            StockAnalysisResult: 统一分析结果
        """
        # 1. 加载数据
        if full_data:
            data = self.data_loader.load_full_data(stock_code)
        else:
            data = self.data_loader.load_light_data(stock_code)

        # 2. 长线分析
        long_term_result = self.long_term.analyze(data)

        # 3. 短线分析
        short_term_result = self.short_term.analyze(data)

        # 4. 事件风险评估
        event_risk_result = self.event_risk.analyze(data)

        # 5. 推荐资格判断
        recommend_result = self.gate.evaluate(
            long_term_result, short_term_result, event_risk_result
        )

        # 6. 生成文本结论
        summary = self.gate.generate_summary(
            long_term_result, short_term_result, event_risk_result, recommend_result
        )
        recommendation = self.gate.generate_recommendation(recommend_result)

        # 7. 合并优势和风险
        strengths = []
        strengths.extend(long_term_result.get('strengths', []))
        strengths.extend(short_term_result.get('strengths', []))

        risks = []
        risks.extend(long_term_result.get('risks', []))
        risks.extend(short_term_result.get('risks', []))
        risks.extend(event_risk_result.get('risks', []))

        # 8. 组装统一结果
        trade_date = data.get('trade_date', '')
        if hasattr(trade_date, 'isoformat'):
            trade_date = trade_date.isoformat()

        last_news_time = data.get('last_news_time')
        if last_news_time and hasattr(last_news_time, 'isoformat'):
            last_news_time = last_news_time.isoformat()

        return StockAnalysisResult(
            stock_code=data.get('stock_code', stock_code),
            stock_name=data.get('short_name', ''),
            analysis_date=str(trade_date),
            last_news_time=str(last_news_time) if last_news_time else None,
            long_term_score=long_term_result['long_term_score'],
            short_term_score=short_term_result['short_term_score'],
            scores=ScoreDetail(
                fundamental=long_term_result['fundamental_score'],
                growth=long_term_result['growth_score'],
                valuation=long_term_result['valuation_score'],
                risk=long_term_result['risk_score'],
                capital=short_term_result['capital_score'],
                technical=short_term_result['technical_score'],
                sentiment=short_term_result['sentiment_score'],
                event=short_term_result['event_score'],
            ),
            event_risk=EventRisk(
                score=event_risk_result['event_risk_score'],
                level=event_risk_result['event_risk_level'],
                events=event_risk_result['triggered_events'],
            ),
            recommend=RecommendResult(
                status=recommend_result['status'],
                reason=recommend_result['reason'],
            ),
            summary=summary,
            recommendation=recommendation,
            strengths=strengths,
            risks=risks,
        )

    def analyze_batch(self, stock_codes: List[str], full_data: bool = True) -> List[StockAnalysisResult]:
        """
        批量分析（用于AI推荐筛选）

        Args:
            stock_codes: 股票代码列表
            full_data: 是否加载完整数据

        Returns:
            分析结果列表（失败的股票会被跳过）
        """
        results = []
        for code in stock_codes:
            try:
                result = self.analyze(code, full_data=full_data)
                results.append(result)
            except Exception as e:
                logger.error(f"分析 {code} 失败: {e}")
                continue
        return results

    def analyze_with_cache(self, stock_code: str, cache: dict) -> StockAnalysisResult:
        """
        使用缓存数据进行分析（避免重复加载数据）

        Args:
            stock_code: 股票代码
            cache: 已加载的数据缓存

        Returns:
            StockAnalysisResult
        """
        data = cache.get(stock_code)
        if not data:
            return self.analyze(stock_code)

        # 长线分析
        long_term_result = self.long_term.analyze(data)

        # 短线分析
        short_term_result = self.short_term.analyze(data)

        # 事件风险评估
        event_risk_result = self.event_risk.analyze(data)

        # 推荐资格判断
        recommend_result = self.gate.evaluate(
            long_term_result, short_term_result, event_risk_result
        )

        # 生成文本结论
        summary = self.gate.generate_summary(
            long_term_result, short_term_result, event_risk_result, recommend_result
        )
        recommendation = self.gate.generate_recommendation(recommend_result)

        # 合并优势和风险
        strengths = []
        strengths.extend(long_term_result.get('strengths', []))
        strengths.extend(short_term_result.get('strengths', []))

        risks = []
        risks.extend(long_term_result.get('risks', []))
        risks.extend(short_term_result.get('risks', []))
        risks.extend(event_risk_result.get('risks', []))

        return StockAnalysisResult(
            stock_code=data.get('stock_code', stock_code),
            stock_name=data.get('short_name', ''),
            analysis_date=data.get('trade_date', ''),
            last_news_time=data.get('last_news_time'),
            long_term_score=long_term_result['long_term_score'],
            short_term_score=short_term_result['short_term_score'],
            scores=ScoreDetail(
                fundamental=long_term_result['fundamental_score'],
                growth=long_term_result['growth_score'],
                valuation=long_term_result['valuation_score'],
                risk=long_term_result['risk_score'],
                capital=short_term_result['capital_score'],
                technical=short_term_result['technical_score'],
                sentiment=short_term_result['sentiment_score'],
                event=short_term_result['event_score'],
            ),
            event_risk=EventRisk(
                score=event_risk_result['event_risk_score'],
                level=event_risk_result['event_risk_level'],
                events=event_risk_result['triggered_events'],
            ),
            recommend=RecommendResult(
                status=recommend_result['status'],
                reason=recommend_result['reason'],
            ),
            summary=summary,
            recommendation=recommendation,
            strengths=strengths,
            risks=risks,
        )
