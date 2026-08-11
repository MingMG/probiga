# -*- coding: utf-8 -*-
import unittest

from server.engine.event_risk_engine import EventRiskEngine
from server.engine.recommendation_gate import RecommendationGate
from server.engine.short_term_engine import ShortTermEngine


def _base_data() -> dict:
    return {
        "stock_code": "000001",
        "short_name": "测试股份",
        "trade_date": "2026-05-30",
        "news": {
            "notices": [],
            "news": [],
        },
        "lifting": {
            "has_lifting_soon": False,
            "records": [],
        },
    }


class EngineDateConsistencyTest(unittest.TestCase):
    def test_short_term_event_uses_trade_date_for_recency(self):
        engine = ShortTermEngine()

        same_day = _base_data()
        same_day["trade_date"] = "2026-05-30"
        same_day["news"]["notices"] = [
            {"notice_date": "2026-05-30", "title": "重大合同公告"}
        ]

        stale_day = _base_data()
        stale_day["trade_date"] = "2026-06-03"
        stale_day["news"]["notices"] = [
            {"notice_date": "2026-05-30", "title": "重大合同公告"}
        ]

        self.assertGreater(engine._calc_event(same_day), engine._calc_event(stale_day))

    def test_event_risk_lifting_uses_trade_date_anchor(self):
        engine = EventRiskEngine()
        data = _base_data()
        data["trade_date"] = "2026-05-30"
        data["lifting"] = {
            "has_lifting_soon": True,
            "lift_date": "2026-06-02",
            "amount": 100000000,
            "ratio": 5.0,
            "records": [
                {
                    "lift_date": "2026-06-02",
                    "volume": 10000000,
                    "amount": 100000000,
                    "ratio": 5.0,
                }
            ],
        }

        result = engine.analyze(data)

        self.assertTrue(any(event["type"] == "lifting" for event in result["triggered_events"]))
        self.assertLessEqual(result["event_risk_score"], 60)

    def test_recommendation_gate_notice_uses_analysis_date(self):
        gate = RecommendationGate()
        long_term = {"long_term_score": 75, "strengths": [], "risks": []}
        short_term = {
            "short_term_score": 80,
            "market_mood_score": 60,
            "strengths": [],
            "risks": [],
        }
        event_risk = {
            "event_risk_level": "LOW",
            "event_risk_score": 90,
            "triggered_events": [
                {"type": "notice", "date": "2026-05-30", "title": "公告"}
            ],
            "risks": [],
        }

        result = gate.evaluate(
            long_term,
            short_term,
            event_risk,
            analysis_date="2026-05-30",
        )

        # Date selection is still anchored to analysis_date, but an otherwise
        # incomplete recommendation can no longer become actionable.  The
        # four-part execution contract fails closed before a legacy status is
        # exposed as a trading recommendation.
        self.assertEqual(result["status"], "DATA_BLOCKED")


if __name__ == "__main__":
    unittest.main()
