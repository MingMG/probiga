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
from datetime import date, datetime, timedelta
from typing import List, Optional

from server.api.routers._engine import get_engine
from server.common.pit_facts import (
    PIT_AVAILABLE,
    normalize_decision_at,
    resolve_common_fact_cutoff,
)

from .data_loader import StockDataLoader
from .date_context import coerce_date
from .event_risk_engine import EventRiskEngine
from .long_term_engine import LongTermEngine
from .recommendation_gate import RecommendationGate
from .schemas import StockAnalysisResult, ScoreDetail, EventRisk, RecommendResult
from .short_term_engine import ShortTermEngine

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

    def analyze(
        self,
        stock_code: str,
        full_data: bool = True,
        trade_date: str | None = None,
        *,
        decision_at: datetime | str | None = None,
    ) -> StockAnalysisResult:
        """
        统一分析入口

        Args:
            stock_code: 股票代码
            full_data: 是否加载完整数据（False时使用精简数据）
            trade_date: 分析截止交易日；为空时默认使用最新交易日。

        Returns:
            StockAnalysisResult: 统一分析结果
        """
        # 1. 加载数据
        if trade_date is None and decision_at is None:
            decision_at = datetime.now().replace(microsecond=0)
        normalized_decision = (
            normalize_decision_at(decision_at)
            if decision_at is not None
            else None
        )
        coverage_end = (
            date.fromisoformat(str(trade_date)[:10])
            if trade_date is not None
            else (
                normalized_decision.date()
                if normalized_decision is not None
                else date.today()
            )
        )
        common_cutoff = {
            "status": "DATA_BLOCKED",
            "reason": "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED",
            "fact_cutoff_at": "",
            "receipt_root_hash": "",
        }
        if normalized_decision is not None:
            common_cutoff = resolve_common_fact_cutoff(
                get_engine(),
                codes=[stock_code],
                decision_at=normalized_decision,
                finance_start_date="1900-01-01",
                finance_end_date=coverage_end,
                event_start_date=normalized_decision.date() - timedelta(days=14),
                event_end_date=normalized_decision.date(),
                require_qmt_event_batch=True,
            )
        reader_decision = (
            normalized_decision
            if common_cutoff.get("status") == PIT_AVAILABLE
            else None
        )
        fact_cutoff_at = common_cutoff.get("fact_cutoff_at") or None
        if full_data:
            data = self.data_loader.load_full_data(
                stock_code,
                trade_date,
                use_realtime=trade_date is None,
                strategy_context=True,
                decision_at=reader_decision,
                fact_cutoff_at=fact_cutoff_at,
            )
        else:
            data = self.data_loader.load_light_data(
                stock_code,
                trade_date,
                use_realtime=trade_date is None,
                strategy_context=True,
                decision_at=reader_decision,
                fact_cutoff_at=fact_cutoff_at,
            )
        data["pit_common_cutoff"] = common_cutoff

        # 2. 长线分析
        long_term_result = self.long_term.analyze(data)

        # 3. 短线分析
        short_term_result = self.short_term.analyze(data)

        # 4. 事件风险评估
        event_risk_result = self.event_risk.analyze(data)

        # 5. 推荐资格判断
        analysis_date = data.get('trade_date')
        recommend_result = self.gate.evaluate(
            long_term_result, short_term_result, event_risk_result, analysis_date=analysis_date
        )
        finance_status = str(
            (data.get("finance") or {}).get("pit_status") or "DATA_BLOCKED"
        )
        event_status = str(
            (data.get("news") or {}).get("event_pit_status")
            or "DATA_BLOCKED"
        )
        reference_status = str(
            (data.get("strategy_reference_evidence") or {}).get("status")
            or PIT_AVAILABLE
        )
        if (
            finance_status != PIT_AVAILABLE
            or event_status != PIT_AVAILABLE
            or common_cutoff.get("status") != PIT_AVAILABLE
            or reference_status != PIT_AVAILABLE
        ):
            recommend_result = {
                **recommend_result,
                "status": "SUSPENDED",
                "reason": (
                    "PIT_DATA_BLOCKED：财务、公告或策略参考数据缺少"
                    "同一事实截止时点的不可变证据，禁止进入策略推荐"
                ),
            }

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
                market_mood=short_term_result.get('market_mood_score'),
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

    def analyze_batch(
        self,
        stock_codes: List[str],
        full_data: bool = True,
        trade_date: str | None = None,
        *,
        decision_at: datetime | str | None = None,
    ) -> List[StockAnalysisResult]:
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
                result = self.analyze(
                    code,
                    full_data=full_data,
                    trade_date=trade_date,
                    decision_at=decision_at,
                )
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
        analysis_date = data.get('trade_date')
        recommend_result = self.gate.evaluate(
            long_term_result, short_term_result, event_risk_result, analysis_date=analysis_date
        )
        finance_status = str(
            (data.get("finance") or {}).get("pit_status") or "DATA_BLOCKED"
        )
        event_status = str(
            (data.get("news") or {}).get("event_pit_status")
            or "DATA_BLOCKED"
        )
        if finance_status != PIT_AVAILABLE or event_status != PIT_AVAILABLE:
            recommend_result = {
                **recommend_result,
                "status": "SUSPENDED",
                "reason": (
                    "PIT_DATA_BLOCKED：缓存数据没有决策时点可验证的"
                    "财务与公告修订，禁止进入策略推荐"
                ),
            }

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
            analysis_date=str(coerce_date(data.get('trade_date')) or data.get('trade_date', '')),
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
                market_mood=short_term_result.get('market_mood_score'),
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
