# -*- coding: utf-8 -*-
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from server.common.config import (
    get_api_cache_config,
    get_api_lifespan_config,
    get_api_observability_config,
    get_api_mysql_pool_config,
    get_kline_mysql_url,
    get_minute_mysql_pool_config,
    get_mysql_url,
    get_qmt_history_mysql_url,
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

    def test_qmt_history_mysql_url_prefers_explicit_local_url(self):
        with patch.dict(os.environ, {
            "QMT_HISTORY_MYSQL_URL": "mysql://local-history",
            "MINUTE_MYSQL_URL": "mysql://minute",
        }):
            get_settings.cache_clear()
            self.assertEqual(get_qmt_history_mysql_url(), "mysql://local-history")

    def test_kline_mysql_url_prefers_explicit_url(self):
        with patch.dict(os.environ, {
            "KLINE_MYSQL_URL": "mysql://kline",
            "QMT_HISTORY_MYSQL_URL": "mysql://local-history",
            "MINUTE_MYSQL_URL": "mysql://minute",
            "MYSQL_URL": "mysql://production",
        }):
            get_settings.cache_clear()
            self.assertEqual(get_kline_mysql_url(), "mysql://kline")

    def test_kline_mysql_url_can_share_minute_local_url(self):
        with patch.dict(os.environ, {
            "KLINE_MYSQL_URL": "",
            "QMT_HISTORY_MYSQL_URL": "",
            "MINUTE_MYSQL_URL": "mysql://minute",
            "MYSQL_URL": "mysql://production",
        }):
            get_settings.cache_clear()
            self.assertEqual(get_kline_mysql_url(), "mysql://minute")

    def test_qmt_history_mysql_url_falls_back_to_minute_url_only(self):
        with patch.dict(os.environ, {
            "QMT_HISTORY_MYSQL_URL": "",
            "MINUTE_MYSQL_URL": "mysql://minute",
            "MYSQL_URL": "mysql://production",
        }):
            get_settings.cache_clear()
            self.assertEqual(get_qmt_history_mysql_url(), "mysql://minute")

    def test_qmt_history_mysql_url_does_not_fall_back_to_production(self):
        with patch.dict(os.environ, {
            "QMT_HISTORY_MYSQL_URL": "",
            "MINUTE_MYSQL_URL": "",
            "MYSQL_URL": "mysql://production",
        }):
            get_settings.cache_clear()
            with self.assertRaises(RuntimeError):
                get_qmt_history_mysql_url()

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

    def test_api_observability_config_allows_disabling_slow_logs(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            api_slow_request_ms=-1,
            api_slow_sql_ms=-1,
        )):
            self.assertEqual(get_api_observability_config(), {"slow_request_ms": 0, "slow_sql_ms": 0})

    def test_api_lifespan_config_defaults_to_api_only(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            api_qmt_live_runtime_enabled=False,
        )):
            self.assertEqual(get_api_lifespan_config(), {"qmt_live_runtime_enabled": False})

    def test_api_cache_config_has_safe_lower_bound(self):
        with patch("server.common.config.get_settings", return_value=SimpleNamespace(
            api_cache_max_entries=0,
        )):
            self.assertEqual(get_api_cache_config(), {"max_entries": 32})


if __name__ == "__main__":
    unittest.main()
