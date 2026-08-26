import unittest

from server.api.routers.portfolio_math import (
    portfolio_calc_next_position,
    portfolio_cost_profit,
    portfolio_recalc_cost_from_history,
    portfolio_should_preserve_existing_position,
    portfolio_trade_fee,
)


class PortfolioCostTest(unittest.TestCase):
    def test_watchlist_only_add_preserves_existing_holding(self):
        self.assertTrue(portfolio_should_preserve_existing_position(
            {"shares": 100, "cost_price": 147},
            requested_cost=0,
            requested_shares=0,
            watchlist_only=True,
        ))

    def test_zero_value_duplicate_add_cannot_silently_clear_holding(self):
        self.assertTrue(portfolio_should_preserve_existing_position(
            {"shares": 100, "cost_price": 147},
            requested_cost=0,
            requested_shares=0,
            watchlist_only=False,
        ))

    def test_new_watchlist_row_is_not_mistaken_for_existing_position(self):
        self.assertFalse(portfolio_should_preserve_existing_position(
            None,
            requested_cost=0,
            requested_shares=0,
            watchlist_only=True,
        ))

    def test_buy_increases_shares(self):
        result = portfolio_calc_next_position("buy", 0, 100, 20, 100)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["new_shares"], 200)
        self.assertEqual(result["trade_shares"], 100)

    def test_sell_decreases_shares(self):
        result = portfolio_calc_next_position("sell", 10, 100, 12, 50)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["trade_shares"], 50)
        self.assertEqual(result["new_shares"], 50)

    def test_sell_more_than_holding_caps_at_holding(self):
        result = portfolio_calc_next_position("sell", 10, 100, 12, 200)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["trade_shares"], 100)
        self.assertEqual(result["new_shares"], 0)

    def test_sell_without_holding_returns_error(self):
        result = portfolio_calc_next_position("sell", 0, 0, 12, 100)
        self.assertEqual(result["status"], "error")

    def test_recalc_simple_buy(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 10.0, "shares": 100},
            ]
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["new_shares"], 100)
        self.assertEqual(result["new_cost"], 10.0)

    def test_recalc_buy_then_sell_partial(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 10.0, "shares": 100},
                {"trans_type": "sell", "price": 12.0, "shares": 50},
            ]
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["new_shares"], 50)
        self.assertAlmostEqual(result["new_cost"], 8.0, places=4)

    def test_recalc_buy_sell_buy_t(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 10.0, "shares": 100},
                {"trans_type": "sell", "price": 11.0, "shares": 100},
                {"trans_type": "buy", "price": 10.5, "shares": 100},
            ]
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["new_shares"], 100)
        self.assertAlmostEqual(result["new_cost"], 9.5, places=4)

    def test_recalc_add_position(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 10.0, "shares": 100},
                {"trans_type": "buy", "price": 12.0, "shares": 100},
            ]
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["new_shares"], 200)
        self.assertAlmostEqual(result["new_cost"], 11.0, places=4)

    def test_recalc_full_clear(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 10.0, "shares": 100},
                {"trans_type": "sell", "price": 12.0, "shares": 100},
            ]
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["new_shares"], 0)
        self.assertEqual(result["new_cost"], 0.0)

    def test_recalc_empty_history(self):
        def mock_read(sql, params):
            return []
        result = portfolio_recalc_cost_from_history("000001", mock_read)
        self.assertEqual(result["new_shares"], 0)
        self.assertEqual(result["new_cost"], 0.0)

    def test_recalc_starts_from_latest_position_snapshot(self):
        def mock_read(sql, params):
            self.assertIn("source", sql)
            return [
                {"trans_type": "buy", "price": 40.0, "shares": 5000, "source": "trade"},
                {"trans_type": "sell", "price": 42.0, "shares": 100, "source": "trade"},
                {"trans_type": "buy", "price": 34.5, "shares": 300, "source": "position_add"},
                {"trans_type": "sell", "price": 33.5, "shares": 200, "source": "trade"},
            ]

        result = portfolio_recalc_cost_from_history("600522", mock_read)

        self.assertEqual(result["new_shares"], 100)
        self.assertAlmostEqual(result["new_cost"], 36.5, places=4)

    def test_recalc_starts_from_latest_initial_snapshot(self):
        def mock_read(sql, params):
            return [
                {"trans_type": "buy", "price": 20.0, "shares": 100, "source": "initial"},
                {"trans_type": "sell", "price": 21.0, "shares": 100, "source": "trade"},
                {"trans_type": "buy", "price": 10.0, "shares": 300, "source": "initial"},
                {"trans_type": "buy", "price": 12.0, "shares": 100, "source": "trade"},
            ]

        result = portfolio_recalc_cost_from_history("000001", mock_read)

        self.assertEqual(result["new_shares"], 400)
        self.assertAlmostEqual(result["new_cost"], 10.5, places=4)

    def test_cost_profit_allows_zero_or_negative_cost(self):
        self.assertEqual(portfolio_cost_profit(100, 10, 0), 1000)
        self.assertEqual(portfolio_cost_profit(100, 10, -2), 1200)

    def test_trade_fee_uses_min_commission_and_sell_taxes(self):
        self.assertAlmostEqual(portfolio_trade_fee("buy", 74.52, 100), 5.07452)
        self.assertAlmostEqual(portfolio_trade_fee("buy", 20.9, 200), 5.0418)
        self.assertAlmostEqual(portfolio_trade_fee("sell", 53.88, 200), 10.49576)
