# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from server.api.routers import _engine
from server.common import minute_data


class EnginePoolConfigTest(unittest.TestCase):
    def tearDown(self):
        _engine._ENGINE = None
        minute_data._MINUTE_ENGINE = None

    def test_api_engine_uses_configured_pool_settings(self):
        fake_engine = object()
        with patch("server.api.routers._engine.get_mysql_url", return_value="mysql://primary"), \
             patch("server.api.routers._engine.get_api_mysql_pool_config", return_value={
                 "pool_size": 3,
                 "max_overflow": 1,
                 "pool_recycle": 1800,
             }), \
             patch("server.api.routers._engine.create_engine", return_value=fake_engine) as create_engine_mock:
            self.assertIs(_engine.get_engine(), fake_engine)

        create_engine_mock.assert_called_once_with(
            "mysql://primary",
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=1,
            pool_recycle=1800,
        )

    def test_minute_engine_uses_configured_pool_settings(self):
        fake_engine = object()
        with patch("server.common.minute_data.get_minute_mysql_url", return_value="mysql://minute"), \
             patch("server.common.minute_data.get_minute_mysql_pool_config", return_value={
                 "pool_size": 2,
                 "max_overflow": 1,
                 "pool_recycle": 2400,
             }), \
             patch("server.common.minute_data.create_engine", return_value=fake_engine) as create_engine_mock:
            self.assertIs(minute_data.get_minute_engine(), fake_engine)

        create_engine_mock.assert_called_once_with(
            "mysql://minute",
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=1,
            pool_recycle=2400,
        )


if __name__ == "__main__":
    unittest.main()
