# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from server.engine.stock_analysis_engine import StockAnalysisEngine


class StockAnalysisEngineDateTest(unittest.TestCase):
    def test_analyze_passes_trade_date_into_loader(self):
        mock_data = {
            "stock_code": "000001",
            "short_name": "测试股",
            "trade_date": "2026-06-10",
            "last_news_time": None,
        }
        long_term = {
            "long_term_score": 60,
            "fundamental_score": 61,
            "growth_score": 62,
            "valuation_score": 63,
            "risk_score": 64,
            "strengths": [],
            "risks": [],
        }
        short_term = {
            "short_term_score": 70,
            "capital_score": 71,
            "technical_score": 72,
            "sentiment_score": 73,
            "market_mood_score": 74,
            "event_score": 75,
            "strengths": [],
            "risks": [],
        }
        event_risk = {
            "event_risk_score": 90,
            "event_risk_level": "LOW",
            "triggered_events": [],
            "risks": [],
        }
        recommend = {"status": "ALLOW", "reason": "ok"}

        engine = StockAnalysisEngine()
        with patch.object(engine.data_loader, "load_full_data", return_value=mock_data) as mock_load, \
             patch.object(engine.long_term, "analyze", return_value=long_term), \
             patch.object(engine.short_term, "analyze", return_value=short_term), \
             patch.object(engine.event_risk, "analyze", return_value=event_risk), \
             patch.object(engine.gate, "evaluate", return_value=recommend) as mock_evaluate, \
             patch.object(engine.gate, "generate_summary", return_value="summary"), \
             patch.object(engine.gate, "generate_recommendation", return_value="recommendation"):
            result = engine.analyze("000001", full_data=True, trade_date="2026-06-10")

        mock_load.assert_called_once_with(
            "000001",
            "2026-06-10",
            use_realtime=False,
            strategy_context=True,
            decision_at=None,
            fact_cutoff_at=None,
        )
        mock_evaluate.assert_called_once_with(
            long_term, short_term, event_risk, analysis_date="2026-06-10"
        )
        self.assertEqual(result.analysis_date, "2026-06-10")


if __name__ == "__main__":
    unittest.main()
