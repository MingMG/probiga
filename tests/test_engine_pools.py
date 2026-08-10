# -*- coding: utf-8 -*-
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from server.api.routers import _engine
from server.common import kline_data
from server.common import minute_data


class _DisposableEngine:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


class EnginePoolConfigTest(unittest.TestCase):
    def tearDown(self):
        _engine._ENGINE = None
        kline_data._KLINE_ENGINE = None
        minute_data._MINUTE_ENGINE = None

    def test_api_engine_uses_configured_pool_settings(self):
        fake_engine = object()
        with patch("server.api.routers._engine.get_mysql_url", return_value="mysql://primary"), \
             patch("server.api.routers._engine.get_api_mysql_pool_config", return_value={
                 "pool_size": 3,
                 "max_overflow": 1,
                 "pool_recycle": 1800,
             }), \
             patch("server.api.routers._engine.create_pooled_engine", return_value=fake_engine) as create_engine_mock:
            self.assertIs(_engine.get_engine(), fake_engine)

        create_engine_mock.assert_called_once_with(
            "mysql://primary",
            pool_config={"pool_size": 3, "max_overflow": 1, "pool_recycle": 1800},
        )

    def test_api_engine_initialization_is_thread_safe(self):
        fake_engine = object()

        def _create_engine(*args, **kwargs):
            time.sleep(0.01)
            return fake_engine

        with patch("server.api.routers._engine.get_mysql_url", return_value="mysql://primary"), \
             patch("server.api.routers._engine.get_api_mysql_pool_config", return_value={
                 "pool_size": 3,
                 "max_overflow": 1,
                 "pool_recycle": 1800,
             }), \
             patch("server.api.routers._engine.create_pooled_engine", side_effect=_create_engine) as create_engine_mock:
            with ThreadPoolExecutor(max_workers=8) as pool:
                engines = list(pool.map(lambda _: _engine.get_engine(), range(8)))

        self.assertEqual(create_engine_mock.call_count, 1)
        self.assertTrue(all(engine is fake_engine for engine in engines))

    def test_api_engine_dispose_resets_shared_engine(self):
        fake_engine = _DisposableEngine()
        _engine._ENGINE = fake_engine

        _engine.dispose_engine()

        self.assertTrue(fake_engine.disposed)
        self.assertIsNone(_engine._ENGINE)

    def test_minute_engine_uses_configured_pool_settings(self):
        fake_engine = object()
        with patch("server.common.minute_data.get_minute_mysql_url", return_value="mysql://minute"), \
             patch("server.common.minute_data.get_minute_mysql_pool_config", return_value={
                 "pool_size": 2,
                 "max_overflow": 1,
                 "pool_recycle": 2400,
             }), \
             patch("server.common.minute_data.create_pooled_engine", return_value=fake_engine) as create_engine_mock:
            self.assertIs(minute_data.get_minute_engine(), fake_engine)

        create_engine_mock.assert_called_once_with(
            "mysql://minute",
            pool_config={"pool_size": 2, "max_overflow": 1, "pool_recycle": 2400},
        )

    def test_minute_engine_initialization_is_thread_safe(self):
        fake_engine = object()

        def _create_engine(*args, **kwargs):
            time.sleep(0.01)
            return fake_engine

        with patch("server.common.minute_data.get_minute_mysql_url", return_value="mysql://minute"), \
             patch("server.common.minute_data.get_minute_mysql_pool_config", return_value={
                 "pool_size": 2,
                 "max_overflow": 1,
                 "pool_recycle": 2400,
             }), \
             patch("server.common.minute_data.create_pooled_engine", side_effect=_create_engine) as create_engine_mock:
            with ThreadPoolExecutor(max_workers=8) as pool:
                engines = list(pool.map(lambda _: minute_data.get_minute_engine(), range(8)))

        self.assertEqual(create_engine_mock.call_count, 1)
        self.assertTrue(all(engine is fake_engine for engine in engines))

    def test_minute_engine_dispose_resets_shared_engine(self):
        fake_engine = _DisposableEngine()
        minute_data._MINUTE_ENGINE = fake_engine

        minute_data.dispose_minute_engine()

        self.assertTrue(fake_engine.disposed)
        self.assertIsNone(minute_data._MINUTE_ENGINE)

    def test_kline_engine_uses_configured_pool_settings(self):
        fake_engine = object()
        with patch("server.common.kline_data.get_kline_mysql_url", return_value="mysql://kline"), \
             patch("server.common.kline_data.get_minute_mysql_pool_config", return_value={
                 "pool_size": 2,
                 "max_overflow": 1,
                 "pool_recycle": 2400,
             }), \
             patch("server.common.kline_data.create_pooled_engine", return_value=fake_engine) as create_engine_mock:
            self.assertIs(kline_data.get_kline_engine(), fake_engine)

        create_engine_mock.assert_called_once_with(
            "mysql://kline",
            pool_config={"pool_size": 2, "max_overflow": 1, "pool_recycle": 2400},
        )

    def test_kline_engine_initialization_is_thread_safe(self):
        fake_engine = object()

        def _create_engine(*args, **kwargs):
            time.sleep(0.01)
            return fake_engine

        with patch("server.common.kline_data.get_kline_mysql_url", return_value="mysql://kline"), \
             patch("server.common.kline_data.get_minute_mysql_pool_config", return_value={
                 "pool_size": 2,
                 "max_overflow": 1,
                 "pool_recycle": 2400,
             }), \
             patch("server.common.kline_data.create_pooled_engine", side_effect=_create_engine) as create_engine_mock:
            with ThreadPoolExecutor(max_workers=8) as pool:
                engines = list(pool.map(lambda _: kline_data.get_kline_engine(), range(8)))

        self.assertEqual(create_engine_mock.call_count, 1)
        self.assertTrue(all(engine is fake_engine for engine in engines))

    def test_kline_engine_dispose_resets_shared_engine(self):
        fake_engine = _DisposableEngine()
        kline_data._KLINE_ENGINE = fake_engine

        kline_data.dispose_kline_engine()

        self.assertTrue(fake_engine.disposed)
        self.assertIsNone(kline_data._KLINE_ENGINE)

    def test_kline_router_only_routes_pure_market_bar_queries(self):
        self.assertTrue(kline_data.should_use_kline_engine(
            "SELECT close FROM sm_stock_kline WHERE stock_code = :c"
        ))
        self.assertTrue(kline_data.should_use_kline_engine(
            "SELECT close FROM sm_stock_minute_gml WHERE stock_code = :c"
        ))
        self.assertFalse(kline_data.should_use_kline_engine(
            "SELECT p.stock_code, k.close FROM st_user_portfolio p JOIN sm_stock_kline k ON k.stock_code = p.stock_code"
        ))
        self.assertFalse(kline_data.should_use_kline_engine(
            "SELECT * FROM st_recommended_stocks"
        ))

    def test_capital_flow_router_only_routes_pure_flow_queries(self):
        self.assertTrue(minute_data.should_use_capital_flow_engine(
            "SELECT main_net_inflow FROM sm_stock_capital_flow_daily WHERE stock_code = :c"
        ))
        self.assertTrue(minute_data.should_use_capital_flow_engine(
            "SELECT main_net_inflow FROM sm_stock_capital_flow_min WHERE stock_code = :c"
        ))
        self.assertFalse(minute_data.should_use_capital_flow_engine(
            "SELECT p.stock_code, f.main_net_inflow FROM st_user_portfolio p "
            "JOIN sm_stock_capital_flow_daily f ON f.stock_code = p.stock_code"
        ))
        self.assertFalse(minute_data.should_use_capital_flow_engine(
            "SELECT * FROM st_user_portfolio"
        ))


if __name__ == "__main__":
    unittest.main()
