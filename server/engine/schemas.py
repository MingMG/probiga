# -*- coding: utf-8 -*-
"""
统一数据结构定义

整个系统必须保证：
同一只股票在 AI推荐、自选股、全市场、个股详情四个页面看到的是同一份分析结果。
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class ScoreDetail(BaseModel):
    """各维度评分详情（0-100）"""

    # 长线维度
    fundamental: Optional[float] = Field(None, description="基本面评分(0-100)")
    growth: Optional[float] = Field(None, description="成长性评分(0-100)")
    valuation: Optional[float] = Field(None, description="估值评分(0-100)")
    risk: Optional[float] = Field(None, description="风险评分(0-100,越高越安全)")

    # 短线维度
    capital: Optional[float] = Field(None, description="资金面评分(0-100)")
    technical: Optional[float] = Field(None, description="技术面评分(0-100)")
    sentiment: Optional[float] = Field(None, description="情绪面评分(0-100)")
    market_mood: Optional[float] = Field(None, description="市场情绪评分(0-100)")
    event: Optional[float] = Field(None, description="消息面评分(0-100)")


class EventRisk(BaseModel):
    """事件风险评估"""

    score: float = Field(0, description="风险评分(0-100, 越低越危险)")
    level: str = Field(
        "DATA_BLOCKED",
        description="风险等级: LOW/MEDIUM/HIGH/CRITICAL/DATA_BLOCKED",
    )
    events: List[dict] = Field(default_factory=list, description="触发事件列表")


class RecommendResult(BaseModel):
    """推荐结果"""

    status: str = Field(
        "DATA_BLOCKED",
        description="推荐状态: ALLOW/SUSPENDED/BLOCK/DATA_BLOCKED",
    )
    reason: str = Field("", description="状态原因")


class StockAnalysisResult(BaseModel):
    """
    统一分析结果

    整个系统所有模块共用此结构，保证同一只股票在任何页面看到的分析结果一致。
    """

    # 基本信息
    stock_code: str
    stock_name: str
    analysis_date: str
    last_news_time: Optional[str] = None

    # 综合评分
    long_term_score: Optional[float] = Field(None, description="长线评分(0-100)")
    short_term_score: Optional[float] = Field(None, description="短线评分(0-100)")

    # 子维度评分
    scores: ScoreDetail = Field(default_factory=ScoreDetail)

    # 事件风险
    event_risk: EventRisk = Field(default_factory=EventRisk)

    # 推荐状态
    recommend: RecommendResult = Field(default_factory=RecommendResult)

    # 文本结论
    summary: str = Field("", description="一句话总结")
    recommendation: str = Field("", description="操作建议")
    strengths: List[str] = Field(default_factory=list, description="优势列表")
    risks: List[str] = Field(default_factory=list, description="风险列表")

    def to_dict(self) -> dict:
        """转换为字典，用于API返回"""
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "analysis_date": self.analysis_date,
            "last_news_time": self.last_news_time,
            "long_term_score": self.long_term_score,
            "short_term_score": self.short_term_score,
            "scores": self.scores.model_dump(),
            "event_risk": self.event_risk.model_dump(),
            "recommend": self.recommend.model_dump(),
            "summary": self.summary,
            "recommendation": self.recommendation,
            "strengths": self.strengths,
            "risks": self.risks,
        }
