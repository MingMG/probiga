# -*- coding: utf-8 -*-
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from server.common.config import (
    get_api_mysql_pool_config,
    get_minute_mysql_pool_config,
    get_mysql_url,
    get_scheduler_runtime_config,
    get_settings,
    get_wecom_webhook,
)


class ConfigTest(unittest.TestCase):
    def tearDown(self):
        get_settings.cache_clear()

    def test_mysql_url_prefers_mysql_url(self):
        with patch.dict(os.environ, {"MYSQL_URL": "mysql://primary", "DATABASE_URL": "mysql://fallback"}):
            get_settings.cache_clear()
            self.assertEqual(get_mysql_url(), "mysql://primary")

    def test_mysql_url_falls_back_to_database_url(self):
        with patch.dict(os.environ, {"MYSQL_URL": "", "DATABASE_URL": "mysql://fallback"}):
            get_settings.cache_clear()
            self.assertEqual(get_mysql_url(), "mysql://fallback")

    def test_mysql_url_required_raises_when_missing(self):
        with patch.dict(os.environ, {"MYSQL_URL": "", "DATABASE_URL": ""}):
            get_settings.cache_clear()
            with self.assertRaises(RuntimeError):
                get_mysql_url(required=True)

    def test_wecom_briefing_falls_back_to_default(self):
        with patch.dict(os.environ, {"WECOM_WEBHOOK_URL": "https://default", "WECOM_BRIEFING_WEBHOOK_URL": ""}):
            get_settings.cache_clear()
            self.assertEqual(get_wecom_webhook("briefing"), "https://default")

    def test_wecom_news_can_use_dedicated_webhook(self):
        with patch.dict(os.environ, {"WECOM_WEBHOOK_URL": "https://default", "WECOM_NEWS_WEBHOOK_URL": "https://news"}):
            get_settings.cache_clear()
            self.assertEqual(get_wecom_webhook("news"), "https://news")

    def test_api_mysql_pool_config_uses_small_machine_defaults(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            api_mysql_pool_size=0,
            api_mysql_max_overflow=-3,
            api_mysql_pool_recycle=120,
        )):
            self.assertEqual(
                get_api_mysql_pool_config(),
                {"pool_size": 1, "max_overflow": 0, "pool_recycle": 300},
            )

    def test_minute_mysql_pool_config_reads_explicit_values(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            minute_mysql_pool_size=4,
            minute_mysql_max_overflow=2,
            minute_mysql_pool_recycle=2400,
        )):
            self.assertEqual(
                get_minute_mysql_pool_config(),
                {"pool_size": 4, "max_overflow": 2, "pool_recycle": 2400},
            )

    def test_scheduler_runtime_config_defaults_to_safe_limits(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            api_embedded_scheduler_enabled=False,
            api_scheduler_max_concurrent_tasks=0,
            api_scheduler_poll_seconds=5,
        )):
            self.assertEqual(
                get_scheduler_runtime_config(),
                {
                    "embedded_enabled": False,
                    "max_concurrent_tasks": 1,
                    "poll_seconds": 15,
                },
            )


if __name__ == "__main__":
    unittest.main()
