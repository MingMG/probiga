# -*- coding: utf-8 -*-
import unittest

from biz.review.generate import (
    DAILY_REVIEW_EXTRA_COLUMNS,
    _determine_main_line,
    classify_sentiment_cycle,
    ensure_daily_review_columns,
)


class _FakeResult:
    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if sql.startswith("SHOW COLUMNS"):
            return _FakeResult()
        if "`highest_board_stocks`" in sql:
            raise RuntimeError("ddl failed")
        return _FakeResult()


class _FakeBegin:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self):
        self.conn = _FakeConn()

    def begin(self):
        return _FakeBegin(self.conn)


class ReviewGenerateTest(unittest.TestCase):
    def test_ensure_daily_review_columns_continues_after_column_failure(self):
        engine = _FakeEngine()

        ensure_daily_review_columns(engine)

        self.assertIn("TEXT", DAILY_REVIEW_EXTRA_COLUMNS["highest_board_stocks"])
        self.assertTrue(any("market_emotion_json" in sql for sql in engine.conn.statements))

    def test_determine_main_line_prefers_rank_and_fund_resonance(self):
        main = _determine_main_line(
            {"score": 66},
            {
                "sector_rank_top": [
                    {"name": "short squeeze", "change_pct": 3.2},
                    {"name": "AI software", "change_pct": 2.8},
                ],
                "sector_fund_flow_top": [
                    {"name": "AI software", "main_net_inflow": 8.5, "change_pct": 2.8},
                    {"name": "chip equipment", "main_net_inflow": 5.2, "change_pct": 1.6},
                ],
                "sector_weak_top": [],
            },
            {"limit_up_count": 58},
        )

        self.assertEqual(main["name"], "AI software")
        self.assertGreaterEqual(main["purity_score"], 75)
        self.assertEqual(main["purity_level"], "高")
        self.assertIn("主线纯正性", main["desc"])


    def test_classify_sentiment_cycle_matches_stock_txt_thresholds(self):
        main_up = classify_sentiment_cycle(
            limit_up_count=35,
            broken_rate=20,
            up_count=3000,
            down_count=1500,
        )
        recovery = classify_sentiment_cycle(
            limit_up_count=20,
            broken_rate=28,
            up_count=2600,
            down_count=1500,
        )
        divergence = classify_sentiment_cycle(
            limit_up_count=28,
            broken_rate=40,
            up_count=2100,
            down_count=1900,
        )
        ebb = classify_sentiment_cycle(
            limit_up_count=8,
            broken_rate=55,
            up_count=900,
            down_count=3200,
        )

        self.assertEqual(main_up["phase"], "主升期")
        self.assertEqual(recovery["phase"], "复苏期")
        self.assertEqual(divergence["phase"], "分化期")
        self.assertEqual(ebb["phase"], "退潮期")


if __name__ == "__main__":
    unittest.main()
